#!/usr/bia/env python

"""
Realiza Download dos resultados do Alice Nacional

"""

import fs
#import webdavfs as wdv
import os
import sys
import json
import logging
import configparser
import hashlib
import argparse
import traceback

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, MetaData, Table, Column, select, update, insert, func, and_, or_, not_
from datetime import datetime, date, timedelta

sys.path.append("..")
import alice_util

__author__ = "Ilueny Santos <iluenysantos@tce.rn.gov.br>, Eduardo Lima <eduardolima@tce.rn.gov.br>, Arthur Mendonça <arthurmendonca@tce.pe.gov.br> e Patrícia Lustosa <patricialustosa@tce.pe.gov.br>"
__copyright__ = "TCE/RN e TCE/PE"
__version__ = '0.2-BETA'


class Downloader(object):
    """Classe que executa download dos resultados do projeto Alice Nacional.

        Attributes:
            config (str): Arquivo de configuração.
            args: argumentos de linha de comando. 
    """

    def __init__(self, config, args):  
        # Obtém seções do arquivo de configuração
        self.config_repo_remoto = config['repositorio_remoto']
        if config.has_section('repositorio_local'):
            self.config_repo_local = config['repositorio_local']
        else:
            self.config_repo_local = None
        self.config_certificado = config['certificado'] if 'certificado' in config else False
        self.config_email = config['email'] if 'email' in config else False
        self.config_variaveis_ambiente = config['variaveis_ambiente'] if 'variaveis_ambiente' in config else False
        
        #Configura o log 
        alice_util.configurar_log(self.config_email, 'downloader')

        #Configura as variáveis de ambiente
        alice_util.configurar_variaveis_ambiente(self.config_variaveis_ambiente)

        # Carrega os certificados SSL quando especificado
        alice_util.carregar_certificado(self.config_certificado)

        # Inicializando configurações relativas ao banco de dados local
        self.config_banco = self.engine = self.schema = self.metadados = self.tabelas = None
        if (config.has_section('metadados_banco')):
            self.config_banco       = config['metadados_banco']
            self.engine             = create_engine(self.config_banco['url'])
            self.schema             = self.config_banco['schema']
            self.metadados = MetaData(bind=self.engine, schema=self.schema)
            self.tabelas = dict([(chave, valor) for chave, valor in self.config_banco.items() if 'tabela_' in chave])
        
        # Obter file systems local e remoto        
        if self.config_repo_local != None:
            try:   
                self.fs_local = alice_util.obter_filesystem(self.config_repo_local)
            except Exception as e:
                logging.error('Erro ao ler repositório local: ' + str(e))
                traceback.print_exc(file=sys.stdout)
                sys.exit()
        else:
            self.fs_local = None
            
        try:
            self.fs_remoto = alice_util.obter_filesystem(self.config_repo_remoto)
            print(self.fs_remoto.listdir('./'))
        except Exception as e:
            logging.error('Erro ao ler repositório remoto: ' + str(e))
            traceback.print_exc(file=sys.stdout)
            sys.exit()
        # Faz verificações e tratamentos acerca dos argumentos de linha de comando do tipo data 
        try:
            args = alice_util.tratar_datas(args)
        except Exception as e:
            logging.error(str(e))
            sys.exit()
        self.data_inicio        = args.data_inicio
        self.data_fim           = args.data_fim

        if self.data_inicio and self.data_fim:
            self.periodo = [(self.data_inicio + timedelta(days=x)).strftime("%Y%m%d") for x in range(0, (self.data_fim - self.data_inicio).days + 1)]

        self.sobrescrever = args.sobrescrever

    def execute(self):
        """Realiza download e carga dos dados em banco de dados.
        
        Returns:
            int: Total de registros inseridos no banco.
        """
        #logging.info("Início de carga de resultados no banco de dados")
        carregar_banco = carregar_local = True
        if (self.config_banco == None): 
            carregar_banco = False
        if (self.config_repo_local == None):
            carregar_local = False
        try:
            conteudo_dv = self.fs_remoto.listdir('./resultados/')
            id_carga_atual = None

            conteudo_logs = self.fs_remoto.listdir('./logs/')
            fs_remoto_log = self.fs_remoto.opendir('logs/')

            if carregar_local:
                self.fs_local.makedirs('logs', recreate=True)

            for dia in self.periodo:
                logging.info('Realizando a carga do dia {}'.format(dia))
                
                #Carregando os arquivos de log
                csvlog = '{}.csv'.format(dia)
                if carregar_banco:
                    # Registrando o início da carga na tabela de controle
                    id_carga_atual = self.registrar_carga(data_carga_inicio=dia)

                    if (id_carga_atual != None):
                        logging.info("Carregando o log do Alice do dia {} no banco de dados".format(dia))
                        if csvlog in conteudo_logs:
                            self.import_db(fs_remoto_log, csvlog, id_carga_atual, True)
                        else:
                            logging.info("Arquivo de log do Alice do dia {} não encontrado".format(dia))
                if carregar_local:
                    #Carregando log:
                    logging.info("Carregando o log do dia {} no sistema de arquivos local".format(dia))
                    if csvlog in conteudo_logs:
                        if self.fs_local.exists('logs/' + csvlog) and self.sobrescrever:
                            self.fs_local.remove('logs/' + csvlog)
                        if not self.fs_local.exists('logs/' + csvlog):
                            fs.copy.copy_file(fs_remoto_log, csvlog, self.fs_local, 'logs/' + csvlog)

                #Carregando alertas e licitacoes:
                if dia in conteudo_dv:
                    fs_remoto_dia = self.fs_remoto.opendir('resultados/{}'.format(dia))
                    fs_remoto_dia_conteudo = fs_remoto_dia.listdir('./')

                    # Se os parâmetros de configuração do banco foram passados no arquivo, realiza a carga dos .csv no banco
                    if (carregar_banco and id_carga_atual != None):
                        logging.info("Iniciando carga do dia {} no banco de dados".format(dia))
                        #Carregando conteúdo da pasta do dia    
                        for f in fs_remoto_dia_conteudo:
                            if f.split('.')[1] in ['csv']:
                                logging.info("Iniciando carga do arquivo {}".format(f))
                                self.import_db(fs_remoto_dia, f, id_carga_atual)
                        
                        #Registrando fim de carga com sucesso no banco de dados:
                        self.registrar_carga(id_carga_fim=id_carga_atual)
                         
                    # Se os parâmetros de um sistema de arquivos local foram passados, realizar a cópia de todos os arquivos
                    if (carregar_local):
                        #Carregando arquivos:
                        fs_local_conteudo = self.fs_local.listdir('./')
                        if (dia + '.ok') not in fs_local_conteudo or self.sobrescrever:
                            logging.info("Iniciando carga do dia {} no sistema de arquivos local".format(dia))
                            # Removendo pasta e arquivo .ok, caso existam
                            if self.fs_local.exists(dia):
                                self.fs_local.removetree(dia)
                            if self.fs_local.exists(dia + '.ok'):
                                self.fs_local.remove(dia + '.ok')

                            # Recriando pasta, carregando conteúdo a partir do Disco Virtual e registrando arquivo .ok
                            self.fs_local.makedirs(dia, recreate=True)
                            copiar_arquivos(fs_remoto_dia, '', self.fs_local, dia)
                            self.fs_local.create(dia + '.ok')
                            logging.info("Download realizado com sucesso: {}".format(dia))
                        else:
                            logging.warning("Alertas do dia {} já foram carregados no sistema de arquivos local".format(dia))
                else:
                    logging.warning('Dia {} não será carregado, a pasta não foi encontrada no Disco Virtual'.format(dia))
        except Exception as e:
            logging.error("Erro: {}".format(str(e)))
            sys.exit()

    def preparar_carga(self, data_carga:int):
        """
        Remove eventuais resquícios de cargas falhas anteriores e inicia um novo registro na tabela de controle
        :param data_carga (int): Data do lote de carga. Ex: 20190131
        """
        logging.info("Iniciando preparação para carga")
        # Conectando ao banco de dados e iniciando a transação       
        connection = self.engine.connect()
        trans = connection.begin()
        iniciar_carga = True
        try:
            # Se a tabela de controle de carga existe, inicia a preparação, caso contrário, levanta exceção
            if self.engine.dialect.has_table(self.engine, self.config_banco['tabela_controle_carga'], schema = self.schema):
                logging.info("Verificando se a última carga foi completada")
                # Carregando objeto da tabela de controle 
                tc = Table(self.config_banco['tabela_controle_carga'], self.metadados, autoload=True)
                # SELECT * FROM <tabela_controle_carga> WHERE data_carga = <valor do parâmetro> ORDER BY id_controle_carga DESC
                s = select(['*']).where(tc.c.data_carga == data_carga).order_by(tc.c.id_controle_carga.desc())
                ultima_carga = connection.execute(s).fetchone()
                # Verifica se há registro da última carga, de modo a limpar resultados intermediários ou sobrescrever registros na nova carga
                if ultima_carga != None:
                    # Confere se essa última carga não foi finalizada com sucesso
                    if ultima_carga['data_fim'] == None or self.sobrescrever:
                        logging.info("Removendo registros da carga anterior (parcial ou a ser sobrescrita)")
                        # Extrai o identificador único da última carga
                        id_controle_carga = ultima_carga['id_controle_carga']    
                        # Itera sobre as tabelas que recebem o carregamento de arquivos (alertas, erros, licitacoes) 
                        for nome_tabela in [valor for (chave, valor) in self.tabelas.items() if chave != 'tabela_controle_carga']:
                            # Se a tabela existe no arquivo de configuração, tenta remover os registros relativos à ultima carga
                            if self.engine.dialect.has_table(self.engine.connect(), nome_tabela, schema = self.schema):
                                logging.info("Início de limpeza de tabela {}".format(nome_tabela))
                                objeto_tabela = Table(nome_tabela, self.metadados, autoload=True)
                                if (self.sobrescrever):
                                    query_remocao = objeto_tabela.delete().where(objeto_tabela.c.data_carga == data_carga)
                                else:
                                    query_remocao = objeto_tabela.delete().where(objeto_tabela.c.id_controle_carga == id_controle_carga)
                                rc = connection.execute(query_remocao).rowcount
                                logging.info("{} registros marcados para remoção na tabela {}".format(rc, nome_tabela))
                        trans.commit()
                        logging.info("Remoção de registros da carga anterior concluída")
                    elif ultima_carga['data_fim'] != None and not(self.sobrescrever):
                        iniciar_carga = False
                        logging.info("Os resultados do dia {} já foram carregados anteriormente, encerrando preparação".format(data_carga))  
                return iniciar_carga
            else:
                raise Exception("Tabela de controle ""{}"" não existe no banco de dados".format(self.config_banco['tabela_controle_carga']))
        except Exception as e:
            trans.rollback()
            raise Exception("Erro ao realizar preparação de carga: {}".format(e))
        finally:
            connection.close()
        
    def import_db(self, filesystem, file_name, id_controle_carga:int, log=False): # TODO: Remover geração de chave e verificar o id_controle_carga
        """
        Grava arquivo com resultados do Alice no Banco

        Args:
        :param filesystem: Objeto de sistema de arquivo que contém o arquivo a ser carregado no SGBD.
        :param file_name (str): Caminho completo do arquivo que será carregado no SGBD.
        Returns:
        :return qtd: Total de registros inseridos no banco (int)
        """    
        # Conectando ao banco de dados e iniciando a transação    
        connection = self.engine.connect() 
        trans = connection.begin()
        
        try:            
            colunas = dict()
            tabela = ''
            result = 0

            if log:
                tabela = self.config_banco['tabela_log']
                colunas = eval(self.config_banco['colunas_log'])
            elif 'alertas' in file_name:
                tabela = self.config_banco['tabela_alertas']
                colunas = eval(self.config_banco['colunas_alertas'])
            elif 'licitacoes' in file_name:
                tabela = self.config_banco['tabela_licitacoes']
                colunas = eval(self.config_banco['colunas_licitacoes'])
            else:
               logging.error("Arquivo não possui tabela mapeada no arquivo de configuração e não será carregado: {}".format(file_name))
            
            with filesystem.open(file_name, encoding = 'utf-8') as arquivo:
                df_result = pd.read_csv(arquivo, 
                    #dtype=colunas,
                    delimiter=";",
                    skip_blank_lines=True,
                    usecols=colunas.keys(),
                    #names=colunas.values()
                    ) #,encoding='latin1'
            if not(df_result.empty):
                # Removendo linhas em branco
                df_result = df_result.dropna(how='all')
                # Ordenando as colunas do dataframe de acordo com a ordem definida no mapeamento do config
                df_result = df_result[colunas.keys()]
                # Renomeando as colunas para o valor mapeado no config
                df_result.columns = colunas.values()
                # Realizando a inserção:
                logging.info("Inserindo dados do arquivo {} na tabela {}".format(file_name, tabela))
                # Adição da chave da tabela de controle de carga
                df_result['id_controle_carga'] = id_controle_carga
                df_result = df_result.astype(dtype= {"id_controle_carga":"int64"})
                # Inserindo resultados na tabela correspondente. O Mapeamento é realizado pelo nome da coluna.
                df_result.to_sql(name=tabela, con=connection, schema=self.schema, if_exists='append', index=False)
                result = len(df_result.index)
                #logging.info("Arquivo carregado: {0} - {1} linha(s) inserida(s)".format(file_name, result))
            trans.commit()
            connection.close()
            logging.info("Arquivo carregado: {0} - {1} linha(s) inserida(s)".format(file_name, result))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            trans.rollback()
            raise Exception("Erro ao importar arquivo '{0}' no banco de dados: {1}".format(file_name, str(e)))
        finally:
            connection.close()

    def registrar_carga(self, data_carga_inicio=None, id_carga_fim=None):
        """
        Registra o início ou fim de uma carga dos resultados do Alice no banco de dados
        Args:
        args ([str]): command line parameters as list of strings
        Returns:
        :id_carga: abc
        """
        id_carga_retorno = None
        if data_carga_inicio == id_carga_fim == None:
            raise Exception("Erro na passagem de argumentos para função registrar_carga")
        else:
            connection = self.engine.connect()
            trans = connection.begin()
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tabela_controle = Table(self.config_banco['tabela_controle_carga'], self.metadados, autoload=True)
            if data_carga_inicio != None:
                iniciar_carga = self.preparar_carga(data_carga_inicio)
                if iniciar_carga:
                    logging.info("Registrando o início da nova carga na tabela de controle")
                    connection.execute(tabela_controle.insert().values(data_carga=data_carga_inicio, data_inicio=agora))
                    logging.info("Novo registro de carga: data_carga = {}, data_inicio = {}".format(data_carga_inicio, agora))
                    # Extraindo o id (autoincremento) da carga atual e retornando
                    get_id_atual = select([func.max(tabela_controle.c.id_controle_carga).label('id_carga_atual')]).where(and_(tabela_controle.c.data_carga == data_carga_inicio, tabela_controle.c.data_inicio == agora))
                    id_carga_retorno = connection.execute(get_id_atual).fetchone()['id_carga_atual']
                else:
                    logging.warning("Dia {} não será carregado".format(data_carga_inicio))
            elif id_carga_fim != None:
                logging.info("Registrando o final da carga na tabela de controle")
                connection.execute(tabela_controle.update().where(tabela_controle.c.id_controle_carga == id_carga_fim).values(data_fim = agora))
                logging.info("Tabela de controle atualizada: id_controle_carga = {}, data_fim = {}".format(id_carga_fim, agora))
                id_carga_retorno = id_carga_fim
            trans.commit()
            connection.close()
        return id_carga_retorno

def copiar_arquivos(fs_origem, dir_origem, fs_destino, dir_destino):
    """
    Copia todos os arquivos de um diretório para outro filesystem
    """    
    logging.info("Início de cópia de arquivos entre os filesystems {} e {}".format(fs_origem, fs_destino))
    arquivos = fs_origem.listdir(dir_origem)
    for arquivo in arquivos:
        path_origem = dir_origem + '/' + arquivo
        path_destino = dir_destino + '/' + arquivo
        if (fs_origem.isfile(path_origem)):
            fs.copy.copy_file(fs_origem, path_origem, fs_destino, path_destino)
            logging.info("Arquivo {} copiado".format(arquivo))
        elif (fs_origem.isdir(path_origem)):
            raise fs.errors.FileExpected(path_origem)
    logging.info("Fim da cópia de arquivos")


def tratar_argumentos(args) :
	"""
    Parse command line parameters

	Args:
	args ([str]): command line parameters as list of strings

	Returns:
	:obj:`argparse.Namespace`: command line parameters namespace
	"""
	parser = argparse.ArgumentParser(
		description="módulo downloader do projeto alice.")
	parser.add_argument(
		"--config",
        required=True,
		help="arquivo de configuração.",
		type=str,
		metavar="str"
        )
	parser.add_argument(
		'--data',
        help='processa objetos relativos à data informada.',
        type=alice_util.converter_data,
        metavar="<DATA>"
		)        
	parser.add_argument(
		'--data-inicio',
        help="importa resultados a partir da data de início informada.",
        type=alice_util.converter_data,
        metavar="<DATA>"
		)              
	parser.add_argument(
		'--data-fim',
        help="importa resultados até uma data de fim informada.",
        type=alice_util.converter_data,
        metavar="<DATA>"
	    )
	parser.add_argument(
        '--sobrescrever',
        action='store_true',
        help = 'se presente, sobrescreve as pastas e arquivos já enviados para as mesmas datas'
        )
	parser.add_argument(
        '--version',
        action='version',
        version='alice_downloader {ver}.'.format(ver=__version__)
        )
	return parser.parse_args(args)

def main(args):
    """
    Principal ponto de entrada. Permite chamadas externas.

    Args:
    args ([str]): Lista de parâmetros da linha de comando.
    """
    args = tratar_argumentos(args)
    print(args)

    config = configparser.ConfigParser()
    config.read(args.config, encoding='utf-8')

    d = Downloader(config, args)
    d.execute()
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
    