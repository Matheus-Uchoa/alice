#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
import fs
from fs import compress
import os
import configparser
import sqlalchemy
import pandas as pd
import tempfile
import argparse
import sys
import shutil
import logging
import logging.handlers
import datetime
import numpy
from datetime import datetime, date, timedelta, time
import traceback
import urllib.request
import ast

sys.path.append("..")
import alice_util

__author__ = 'edansfs@tcu.gov.br, arthurmendonca@tce.pe.gov.br e patricialustosa@tce.pe.gov.br'

_DESCRICAO = "Executa uma carga do Alice Nacional."

_EPILOGO = """obs.: O parâmetro <DATA> pode ser especificado em dois formatos possíveis:
      - Data exata. ex.: 17/08/2018
      - Data relativa: ex.: 10 (dez dias atrás); 0 (dia de hoje); 1 (ontem)
      Se nenhum parâmetro de data for fornecido, serão processados apenas
      objetos de hoje.
"""

class Uploader(object):
    """Classe que executa upload dos metadados e arquivos para o projeto Alice Nacional.

        Attributes:
            config (str): Arquivo de configuração.
            args: argumentos de linha de comando. 
    """

    def __init__(self, config, args):

        # Obtém sessões do arquivo de configuração
        self.config_origem_metadados = config['origem_metadados']
        self.config_origem_arquivos = config['origem_arquivos']
        self.config_destino = config['destino']
        self.config_certificado = config['certificado'] if 'certificado' in config else False
        self.config_email = config['email'] if 'email' in config else False
        self.config_variaveis_ambiente = config['variaveis_ambiente'] if 'variaveis_ambiente' in config else False

        #Configura o log 
        alice_util.configurar_log(self.config_email, 'uploader')

        #Configura as variáveis de ambiente
        alice_util.configurar_variaveis_ambiente(self.config_variaveis_ambiente)

        # Carrega os certificados SSL quando especificado
        alice_util.carregar_certificado(self.config_certificado)

        # Faz verificações e tratamentos acerca dos argumentos de linha de comando do tipo data 
        try:
            args = alice_util.tratar_datas(args)
        except Exception as e:
            logging.error(str(e))
            sys.exit()
        
        self.data_inicio        = args.data_inicio
        self.data_fim           = args.data_fim
        
        self.sobrescrever = args.sobrescrever

        # Obtém uma conexão ao banco de dados com os metadados por meio da biblioteca sqlalchemy
        try:
            self.conexao_banco_metadados = sqlalchemy.create_engine(self.config_origem_metadados['banco'])
        except Exception as e:
            logging.error('Erro ao conectar no banco de dados para obter os metadados: {}' + str(e))
            sys.exit()
        try:
            self.conexao_banco_arquivos = sqlalchemy.create_engine(self.config_origem_arquivos['banco'])
        except Exception as e:
            logging.error('Erro ao conectar no banco de dados para obter a lista de arquivos associados às licitações: ' + str(e))
            sys.exit()

        # Obtém parâmetros relacionados aos sistemas de arquivo de origem e destino e uma pasta temporária em que serão escritos os arquivos a serem zipados
        try:
            self.filesystem_arquivos = alice_util.obter_filesystem(self.config_origem_arquivos, criar_diretorio=False)
        except Exception as e:
            logging.error('Erro ao ler pasta com arquivos de origem: ' + str(e))
            traceback.print_exc(file=sys.stdout)
            sys.exit()

        try:
            self.filesystem_destino = alice_util.obter_filesystem(self.config_destino, criar_diretorio=True)
        except Exception as e:
            logging.error('Erro ao ler pasta com arquivos de destino: ' + str(e))
            sys.exit()
        

    def execute(self):
        """Realiza upload dos metadados e arquivos no servidor do Alice Nacional
        """
        os.makedirs('./temp/', exist_ok=True)
        self.filesystem_pasta_temp = fs.open_fs('./temp/')

        self.dados = {}

        #print('Carga de {} a {}'.format(args.data_inicio, args.data_fim))
        # Executa o upload dos metadados e dos arquivos zip para cada dia do intervalo determinado
        for dia in (self.data_inicio + timedelta(n) for n in range(int((self.data_fim - self.data_inicio).days) + 1)):
            diaString = dia.strftime("%d/%m/%Y")
            diaStringInvertido = dia.strftime("%Y%m%d")
        
            caminho_pasta_dia = '/' + diaStringInvertido
            caminho_arquivo_ok = caminho_pasta_dia + '.ok'
            caminho_arquivo_csv = caminho_pasta_dia + '/licitacoes.csv'
            
            # Verificando se o parâmetro de sobrescrever foi passado. 
            # Caso não tenha sido, confere se a pasta ou o arquivo <dia>.ok já existe.
            if (not self.sobrescrever):
                # Verificando se existe arquivo <dia>.ok. 
                # Caso exista, a pasta não é reenviada, pois o dia já foi processado pelo TCU. 
                if self.filesystem_destino.isfile(caminho_arquivo_ok):
                    logging.info("Pasta do dia {} não enviada pois existe arquivo {}.ok na pasta de destino".format(diaString, diaStringInvertido))
                    continue
                # Verificando se existe arquivo licitacoes.csv na pasta do dia. 
                # Caso exista, a pasta não é reenviada, pois o dia já foi enviado ao TCU. 
                if self.filesystem_destino.isfile(caminho_arquivo_csv):
                    logging.info("Pasta do dia {} não enviada pois pasta com as licitações do dia já existe no filesystem de destino".format(diaString))
                    continue
            else:
                logging.info("Carga do dia {} será sobrescrita".format(diaString))
                # Caso esteja sobrescrevendo, remover arquivo data <dia>.ok e a pasta do dia.
                if self.filesystem_destino.isfile(caminho_arquivo_ok):
                    self.filesystem_destino.remove(caminho_arquivo_ok)
                    logging.info("Arquivo {}.ok removido".format(diaStringInvertido))
                if self.filesystem_destino.isdir(caminho_pasta_dia):
                    remover_pasta(self.filesystem_destino, caminho_pasta_dia)
                    logging.info("Pasta {} removida".format(diaStringInvertido))

            logging.info("Início de carga do dia {}".format(diaString))
            # Obtendo dataframe de metadados das licitações:
            try:
                logging.info("Iniciando a obtenção dos metadados das licitações")
                df_licitacoes = self.obter_dataframe(dia, self.conexao_banco_metadados, self.config_origem_metadados, 'licitacoes', True)
                logging.info("Licitações obtidas")
            except Exception as e:
                logging.error("Erro ao carregar os metadados das licitações do dia {}: {}".format(diaString, str(e)))
                sys.exit()
            
            # Obtendo os outros metadados definidos no arquivo de configuração
            for t in ast.literal_eval(self.config_origem_metadados['tabelas']): #TODO: Verificar se precisa do eval ou se o configparser já lê o dict/array
                if t != 'licitacoes':
                    logging.info("Obtendo os dados da tabela {}".format(t))
                    try:
                        df_tabela = self.obter_dataframe(dia, self.conexao_banco_metadados, self.config_origem_metadados, t, True) #TODO: Corrigir problemas na importação
                        lista_elementos = []
                        for id in df_tabela['id_licitacao'].unique():
                            df = df_tabela[df_tabela['id_licitacao']==id]
                            df = df.drop(columns='id_licitacao')
                            lista_elementos.append((id,df))
                        df_tabela = pd.DataFrame(lista_elementos, columns =['id_licitacao', t])
                        df_licitacoes = df_licitacoes.join(df_tabela.set_index('id_licitacao'), on='id_licitacao')
                        print(df_licitacoes)
                    except Exception as e:
                        logging.error("Erro ao carregar metadados da tabela {} na carga do dia {}:{}".format(t,diaString,str(e)))
                        sys.exit()
            # Obtendo lista de arquivos a serem enviados:
            try:
                logging.info("Iniciando a obtenção da lista de arquivos a serem compactados")
                dataframe_arquivos = self.obter_dataframe(dia, self.conexao_banco_arquivos, self.config_origem_arquivos, 'arquivos', False)
                arquivos_zip = self.obter_arquivos_zip(self.filesystem_pasta_temp, self.filesystem_arquivos, dataframe_arquivos, df_licitacoes)
                logging.info("Fim da obtenção dos arquivos")
            except Exception as e:
                logging.error("Erro ao obter a lista de arquivos de origem do dia {}: {}".format(diaString, str(e)))
                sys.exit()
            # Realizando o upload:
            self.upload(df_licitacoes, self.filesystem_pasta_temp, arquivos_zip, self.filesystem_destino, dia)
            logging.info("Fim de carga do dia {} - {} arquivos carregados".format(diaString, len(arquivos_zip)))
        shutil.rmtree('./temp/')



    def obter_dataframe(self, dia, conexao_banco, config_origem, tabela, validar=False): #TODO: Colocar aqui arquivos/metadados. Tem que mudar a lógica.
        """
        Obtém um dataframe de acordo com as configurações especificadas, opcionalmente realiza validação da estrutura
            de acordo com configuração
        :param conexao_banco: engine de conexão ao banco de dados do sqlalchemy
        :param config_origem: contém a configuração referente à consulta sql a ser executada
        :param tipo: tipo do dataframe - se for "Metadados", realiza a validação da estrutura do dataframe antes de retorná-lo
        :return: dataframe do Pandas
        """
        try:
            # Obtém o script SQL para obter os metadados das licitações
            script_sql = self.obter_script_sql(config_origem, tabela)
            # Executa o script SQL na conexão obtida
            df_obtido = pd.read_sql_query(script_sql, conexao_banco, params={'dia': dia})
            if validar and len(df_obtido) > 0:
                self.validar_dataframe(df_obtido, config_origem, tabela)
            self.dados[tabela] = df_obtido
        except Exception as e:
            logging.error('Erro ao consultar banco de dados para obter dataframe: ' + str(e))
            sys.exit()
        return df_obtido
    

    def validar_dataframe(self, df_validar, config_origem, tabela):
        """
        Realiza uma verificação mínima da qualidade dos dados retornados no dataframe (ex. Campos obrigatórios,
            tipos de dados, etc) de acordo com os parâmetros de configuração
        :param df_licitacoes: dataframe a ser validado
        :param config_origem: seção do arquivo de configuração contendo as regras de validação
        return: dataframe do Pandas
        """
        colunas_erro = []
        try:
            #Avalia parâmetros de configuração da tabela - esquema e lista de campos obrigatórios
            logging.info("Validando os dados obtidos da tabela \"{}\"".format(tabela))
            config_esquema = ast.literal_eval(config_origem['esquema_' + tabela])
            
            config_obrigatorios = {key:value for (key,value) in config_esquema.items() if value['obrigatorio'] == 'S'}
            config_obrigatorios = config_obrigatorios.keys()

            #Substituindo strings vazias do dataframe por NaN para usar na verificação de campos nulos.        
            df_tabela = df_validar.replace('', numpy.nan)            
            if not(all(elem in df_validar.columns for elem in config_obrigatorios)): #set(config_obrigatorios).issubset(set(df_validar.columns))):
                #print('validando colunas ausentes na tabela ', tabela)
                colunas_ausentes = set(config_obrigatorios) - set(df_validar.columns)
                colunas_erro.append("O(s) campo(s) \"{}\" são obrigatórios e não estão presentes no conjunto de metadados da tabela \"{}\"".format(",".join(colunas_ausentes), tabela))
            else:
                for col in df_validar.columns:
                    print("validando coluna ", col)
                    if col not in config_esquema.keys():
                        colunas_erro.append("O campo \"{}\" é retornado pela consulta da tabela \"{}\" mas não faz parte da configuração do esquema".format(col, tabela))
                    elif (df_validar[col].isnull().any() and col in config_obrigatorios):
                        #print("validando presenca de nulos em ", col)
                        colunas_erro.append("O campo \"{}\" da tabela \"{}\" é obrigatório, mas há um ou mais registros com valores nulos".format(col, tabela))
                    # Confere se o campo só contém valores nulos, caso em que é impossível realizar a inferência de tipo
                    elif df_validar[col].isnull().all():
                        #print("validando presenca de todos nulos em ", col) 
                        logging.warning("O tipo do campo \"{}\" não pôde ser inferido, pois este só contém valores nulos".format(col))
                    # Confronta tipo da coluna de acordo com definição do esquema
                    else:
                        idx = df_validar.columns.get_loc(col)
                        tipo_col = config_esquema[col]['tipo']
                        tipo_lido = str(df_validar.dtypes[idx])
                        if(tipo_lido in ['int', 'int64'] and tipo_col in ['float','float64']):
                            logging.warning("O campo \"{}\" é do tipo int, mas no modelo de dados consta como float - os dados serão carregados mesmo assim".format(col))
                        elif(tipo_lido=='float64' and tipo_col=='int'):
                            logging.warning("O campo \"{}\" é do tipo float, mas no modelo de dados consta como int - os dados poderão ser truncados no upload para o Alice".format(col))
                        elif(traduz_tipos_pandas(tipo_lido) != tipo_col):
                            colunas_erro.append("O campo \"{}\" é do tipo {}, quando deveria ser do tipo {}".format(col, tipo_lido, tipo_col))  

                    #Confronta os valores válidos da coluna de acordo com definição de valores possíveis do esquema
                    if('enum' in config_esquema[col]): 
                        valoresValidos = config_esquema[col]['enum']
                        if(col not in config_obrigatorios):
                            valoresValidos.append(numpy.nan)
                            df_validar[col].replace([None], numpy.nan, inplace=True)

                        if (not df_validar[col].isin(valoresValidos).all()):
                            colunas_erro.append("Há linhas do campo \"{}\" fora dos valores permitidos para esse campo.".format(col))  

                    #Confronta os valores válidos da coluna de acordo com a chave estrangeira definida no esquema
                    if('ref' in config_esquema[col]):
                        tab = config_esquema[col]['ref'].split('.')[0]
                        campo = config_esquema[col]['ref'].split('.')[1]

                        valoresValidos = self.dados[tab][campo]
                        
                        #if(col not in config_obrigatorios):
                        #    valoresValidos = valoresValidos.append(pd.Series([numpy.NaN]))
                        #    df_validar[col].replace([None], numpy.nan, inplace=True)
                        if (not df_validar.dropna()[col].isin(valoresValidos).all()): 
                            colunas_erro.append("Há linhas do campo \"{}\" com valor não correspondente de chave estrangeira.".format(col))  

        except Exception as e:
            logging.error('Erro ao validar dataframe: ' + str(e))
            sys.exit()
        if(len(colunas_erro) > 0):
            # Se houve divergências 
            logging.error('Validação do dataframe de metadados finalizada com erro: \n' + "\n".join(colunas_erro))
            sys.exit()
        return df_validar


    def obter_script_sql(self, config_banco, tabela):
        """
        Obtém o script sql de uma seção do arquivo de configuração. O arquivo de configruação pode ser escrito dentro
            do arquivo de configuração (campo 'consulta_sql') ou pode ser informado um arquivo externo que contenha o script
        SQL (campo 'arquivo_sql').
        :param config_banco: seção do arquivo de configuração que informa se a consulta está em texto na própria configuração
        ou em um arquivo .sql
        :return: script SQL
        """
        arquivo = 'arquivo_sql_' + tabela
        consulta = 'consulta_sql_' + tabela
        if arquivo in config_banco:
            with open('./sql/'+config_banco[arquivo], encoding='utf8') as f:
                script = f.read()                  
        elif consulta in config_banco:
            script = config_banco[consulta]
        else:
            logging.error('Erro: Informe o campo "consulta_sql" ou "consulta_arquivo" no arquivo de configurações (.ini)')
            sys.exit(1)
        return script


    def obter_arquivos_zip(self, filesystem_pasta_temp, filesystem_arquivos, df_arquivos, df_metadados):
        """
        Retorna uma lista com os arquivos zips de cada licitação que serão copiados para o destino
        :param filesystem_pasta_temp: sistema de arquivos da pasta temporária em que os arquivos serão copiados e compactados
        :param filesystem_arquivos: sistema de arquivos onde os arquivos de origem estarão
        :param df_arquivos: dataframe contendo a lista de arquivos ou pastas associados às licitações
        :param df_metadados: dataframe contendo os metadados das licitações
        :return: lista contendo o nome dos arquivos zips a serem futuramente copiados para o filesystem de destino
        """
        try:
            arquivos_zip = []
            # Primeiro se obtém a lista das licitações cujos metadados foram retornados
            lista_licitacoes = list(df_metadados['id_licitacao'])
            # Itera sobre os ids de licitação, procurando no dataframe de arquivos quais os arquivos correspondentes a cada uma
            for lic in lista_licitacoes:
                # Cria uma pasta com o nome da licitação, em que serão colocados os arquivos a serem zipados
                # Para cada licitação, gera lista com os caminhos dos arquivos associados dentro do diretório do filesystem.
                paths = list(df_arquivos.loc[df_arquivos['id_licitacao'] == lic]['caminho'])
                if len(paths) > 0:
                    filesystem_pasta_licitacao = filesystem_pasta_temp.makedirs(lic, recreate = True)
                    #print("filesystem_pasta_licitacao:", filesystem_pasta_licitacao)
                    # Iterando sobre os arquivos e pastas retornados na consulta, escreve todos na pasta de saída para posteriormente ziar
                    for path in paths:
                        if (path.startswith('http://') or path.startswith('https://')):
                            caminho_origem = path
                        else:
                            caminho_origem = '/' + path
                        nome_arquivo_pasta = get_nome_arquivo_pasta(caminho_origem) #Puxa somente o fim do caminho (nome do arquivo ou pasta)
                        if (caminho_origem.startswith('http://') or caminho_origem.startswith('https://')):
                            #destino = filesystem_pasta_licitacao.getsyspath('.') + '/' + nome_arquivo_pasta
                            #print('download de arquivo via http: ',caminho_origem, ' ; destino: ',destino)
                            try:
                                urllib.request.urlretrieve(caminho_origem, filesystem_pasta_licitacao.getsyspath('.') + '/' + nome_arquivo_pasta)
                            except:
                                print("Erro ao realizar download: ", caminho_origem)
                                #logging.warning("Erro ao realizar download: ", caminho_origem)
                        elif filesystem_arquivos.isfile(caminho_origem):
                            filesystem_pasta_licitacao.create(nome_arquivo_pasta)
                            fs.copy.copy_file(filesystem_arquivos, caminho_origem, filesystem_pasta_licitacao, nome_arquivo_pasta)
                        elif filesystem_arquivos.isdir(caminho_origem):
                            filesystem_pasta_licitacao.makedirs(nome_arquivo_pasta, recreate = True)
                            fs.copy.copy_dir(filesystem_arquivos, caminho_origem , filesystem_pasta_licitacao, nome_arquivo_pasta)         
                        
                        else:
                            pass
                    with filesystem_pasta_temp.open(lic + '.zip', mode='wb') as arquivo_zip:
                        fs.compress.write_zip(filesystem_pasta_licitacao, arquivo_zip)
                    filesystem_pasta_temp.removetree(lic)
                    arquivos_zip.append(lic + '.zip')
                else:
                    logging.info('Nenhum arquivo obtido para a licitação de id {}'.format(lic))
            print(arquivos_zip)
            return arquivos_zip
        except Exception as e:
            logging.error('Erro ao compactar arquivos: {}'.format(str(e)))
            traceback.print_exc(file=sys.stdout)
            sys.exit()


    def upload(self, df_licitacoes, filesystem_pasta_temp, arquivos_zip, filesystem_destino, dia):
        """
        Faz o upload dos metadados e dos arquivos zip das licitações para o sistema de arquivos de destino
        :param df_licitacoes: dataframe contendo os metadados das licitações
        :param filesystem_pasta_temp: sistema de arquivos da pasta temporária que contém os arquivos compactados de cada licitação
        :param arquivos_zip: lista dos arquivos zip que serão enviados no lote
        :param filesystem_destino: sistema de arquivos em que os arquivos compactados serão colocados
        :param dia: a data de publicação das licitações do lote que está sendo enviado 
        """
        diaString = dia.strftime('%d/%m/%Y')
        logging.info('Início de upload dos arquivos do dia {}'.format(diaString))
        try: 
            # Arquivos PDF:
            data_str = dia.strftime('%Y%m%d')
            fs_batch = filesystem_destino.makedirs('/' + data_str, recreate=True)
            for arq in arquivos_zip:
                fs.copy.copy_file(filesystem_pasta_temp, arq, fs_batch, arq)
            # Metadados:
            df_licitacoes = df_licitacoes.replace(r'\n',' ', regex=True) 
            with fs_batch.open('licitacoes.json', mode='w', encoding='utf-8') as arq_meta:
                df_licitacoes.to_json(arq_meta, orient='records', indent=4, double_precision=2,date_format='iso', force_ascii=False)
                #fs_batch.upload('licitacoes.json', arq_meta)
            logging.info('Fim de upload dos arquivos do dia {}'.format(diaString))
        except Exception as e:
            logging.error('Erro ao fazer upload de arquivos: {}'.format(str(e)))
            sys.exit()

def remover_pasta(filesystem, dir_path):
        logging.info("Removendo o diretório {}".format(dir_path))
        with filesystem._lock:
            if filesystem.isempty(dir_path):
                filesystem.removedir(dir_path)
            else:
                conteudo_pasta = filesystem.listdir(dir_path)
                for item in conteudo_pasta:
                    caminho_item = dir_path + '/' + item
                    if filesystem.isfile(caminho_item):
                        filesystem.remove(caminho_item)
                        logging.info('Arquivo removido: {}'.format(caminho_item))
                    elif filesystem.isfolder(caminho_item):
                        if filesystem.isempty(caminho_item):
                            filesystem.removedir(caminho_item)
                            logging.info('Subpasta removida: {}'.format(caminho_item))
                        else:
                            logging.info('Subpasta {} não está vazia, iniciando execução recursiva'.format(caminho_item))
                            remover_pasta(filesystem, caminho_item)
                filesystem.removedir(dir_path)
                logging.info('Pasta {} removida'.format(dir_path))

def traduz_tipos_pandas(tipo_pandas):
    """
    Faz a correspondência entre os nomes dos tipos de dados do pandas e os tipos a serem passados como restrições no 
        arquivo de configuração
    :param tipo_pandas: nome do tipo de dados utilizado pelo pandas
    :return: retorna o valor equivalente 
    """
    if ("object" in tipo_pandas):
        return "str"
    if ("int" in tipo_pandas or "int64" in tipo_pandas):
        return "int"
    if ("float" in tipo_pandas or "float64" in tipo_pandas):
        return "float"
    if ("datetime" in tipo_pandas or "datetime64[ns]" in tipo_pandas):
        return "datetime"
    else:
        return tipo_pandas


def get_nome_arquivo_pasta(path):
    fim_path = None
    if path[-1:] in ['/', '\\']:
        path = path[:-1]
    if '/' in path:
        fim_path = path.rsplit('/', 1)[1]
    elif '\\' in path:
        fim_path = path.rsplit('\\', 1)[1]
    return fim_path

def tratar_argumentos(args):
    """
    Efetua o processamento dos argumentos passados por linha de comando.
    :return: um objeto contendo todos os argumentos tratados
    """
    parser = argparse.ArgumentParser(description=_DESCRICAO,
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     epilog=_EPILOGO)
    parser.add_argument('--config', type=str, default='config.ini',
                        help='arquivo de configuração')
    parser.add_argument('--data', metavar="<DATA>", type=alice_util.converter_data,
                        help='processa objetos relativos à data informada.')
    parser.add_argument('--data-inicio', metavar="<DATA>", type=alice_util.converter_data, 
                        help='processa objetos a partir de uma data de início')
    parser.add_argument('--data-fim', metavar="<DATA>", type=alice_util.converter_data, 
                        help='processa objetos até uma data de fim')
    parser.add_argument('--sobrescrever', action='store_true', 
                        help = 'se presente, sobrescreve as pastas e arquivos já enviados para as mesmas datas')
    args = parser.parse_args(args)

    return args

def main(args):
    """
    Principal ponto de entrada. Permite chamadas externas.

    Args:
    args ([str]): Lista de parâmetros da linha de comando.
    """
    args = tratar_argumentos(args)
    print(args)

    # Carrega o arquivo de configuracao (.ini)
    config = configparser.ConfigParser()
    config.read(args.config, encoding='utf-8')

    u = Uploader(config, args)
    u.execute()
    logging.info("Fim de programa")

def run():
    """
    Ponto de entrada para chamada via console.
    """
    try:
        main(sys.argv[1:])
    except Exception as e:
        logging.error('Erro ao executar upload.py: {}'.format(str(e)))
        traceback.print_exc(file=sys.stdout)
        sys.exit()

if __name__ == "__main__":
    run()
    