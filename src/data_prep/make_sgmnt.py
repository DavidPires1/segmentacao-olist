import os
import sqlalchemy
import argparse
import pandas as pd
import sqlite3
import datetime
import dateutils
from olistlib.db import utils



#Os endereços do nosso projeto e sub pastas
DATA_PREP_DIR = os.path.dirname( os.path.dirname(__file__) ) #diretorio de data_prep
SRC_DIR = os.path.dirname ( DATA_PREP_DIR ) #diretorio de codigo
BASE_DIR  = os.path.dirname( SRC_DIR ) #diretorio raiz do projeto
DATA_DIR = os.path.join( BASE_DIR, 'data') #diretorio de dados


# Parser de data para fazer foto
parser = argparse.ArgumentParser()
parser.add_argument( '--date_end', '-e', help='Data de fim da extração', default = '2018-06-01')
args = parser.parse_args()

date_end = args.date_end

date_init = datetime.datetime.strptime( args.date_end, "%Y-%m-%d") - dateutils.relativedelta( years =+ 1)
date_init = date_init.strftime("%Y-%m-%d")

# Importa a query
query = utils.import_query(os.path.join( DATA_PREP_DIR, 'data_prep/segmentos.sql'))
query = query.format( date_init = date_init, date_end = date_end )

# Abrindo conexão com banco...
conn = utils.connect_db('mysql', os.path.join(BASE_DIR, '.env'))

try:
    create_query = f'''CREATE TABLE Olist.tb_sellers_sgmnt AS\n{query};'''
    utils.execute_many_sql( create_query, conn)
except:
    insert_query = f'''DELETE FROM Olist.tb_sellers_sgmnt WHERE DT_SGMNT = '{date_end}';
    INSERT INTO Olist.tb_sellers_sgmnt{query};'''

    utils.execute_many_sql( insert_query, conn, verbose=True)
