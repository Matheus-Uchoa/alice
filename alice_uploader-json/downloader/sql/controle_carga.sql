CREATE TABLE [alice].[controle_carga](
	[id_controle_carga] [int] IDENTITY(1,1) PRIMARY KEY NOT NULL,
	[data_carga] [datetime] NOT NULL,
	[data_inicio] [datetime] NOT NULL,
	[data_fim] [datetime] NULL
);

