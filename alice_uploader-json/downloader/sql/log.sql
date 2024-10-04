CREATE TABLE [alice].[log](
	[id_controle_carga] [int] NOT NULL,
	[data_carga] datetime NOT NULL,
	[data_log] datetime NOT NULL,
	[id_licitacao] [int] NULL,
	[tipo_log] [varchar](64) NOT NULL,
	[descricao] [varchar](512) NOT NULL
) 
