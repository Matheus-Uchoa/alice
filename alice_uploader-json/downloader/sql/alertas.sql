CREATE TABLE [alice].[alertas](
	--dados do envio e identificacao
	[id_controle_carga] [int] NOT NULL,
	[codigo_tc] [varchar](16) NOT NULL,
	[data_carga] datetime NOT NULL,
	[tipo_informe] [varchar](64) NOT NULL,
	[id_licitacao] [int] NOT NULL,

	--dados gerados pelo alice
	[codigo_tipologia] [varchar](100) NOT NULL,
	[nome_tipologia] [varchar](1024) NOT NULL,
	[alerta] [varchar](max) NOT NULL,
	[risco_alerta] [int] NOT NULL,
	
	--dados enviados para o alice
	[data_publicacao] datetime NULL,
	[objeto] [varchar](max) NULL,
	[nome_licitacao] [varchar](255) NULL,
	[codigo_unidade] [int] NULL,
	[nome_unidade] [varchar](1024) NULL,
	[codigo_modalidade] [varchar](255) NULL,
	[nome_modalidade] [varchar](255) NULL,
);
