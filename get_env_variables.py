import os

os.environ['envn'] = 'dev'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'PySpark App'

current = os.getcwd()

src_olap = current +'\source\olap'

src_oltp = current +'\source\oltp'

trans_path = 'output\out_transportation'
