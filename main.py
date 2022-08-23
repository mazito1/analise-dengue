import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options= PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude'
]

def lista_para_dicionario(elemento, colunas):
    """
    Esse método transforma a lista para dicionário.
    Recebe o parametro "elemento" e "colunas"
    No retorno utilizamos a função ZIP que vai juntar as duas listas (elemento e coluna) e utilizamos também o DICT para transformar em dicionário
    """
    return dict(zip(colunas, elemento))    

def texto_para_lista(elemento, delimitador ='|'): 
    """
    Função para transformar string em lista, os parametros são genericos
    "Elemento" nesse caso seria a informação do csv e o "delimitador" é o que separa as informações no dataset
    """
    return elemento.split(delimitador)

def trata_data(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2]) #ANO_MES receberia ano-mes como uma lista. porém com o .JOIN concatenamos a partir de um -
    return elemento

def chave_uf(elemento):
    """
    Recebe dicionário e retorna uma tupla com estado e elemento (UF, dicionário)
    """
    chave=elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS, [{},{}])
    Retornar uma tupla ('RS-2014-12,8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d',registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}",float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}",0.0)

# dengue funciona como PCOLLECTION, pode ter várias etapas separadas por |
dengue=(
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('./data/casos_dengue.txt', skip_header_lines=1) # ReadFromText lê o arquivo indicado na parametrização, primeiro parametro é o arquivo a ser lido e o segundo indica que irá pular 1 linha (header)
    | "De texto para lista" >> beam.Map(texto_para_lista) ## Ou podemos utilizar a expressão em lambda (lambda x: x.split('|'))
    | "Lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ANO_MES" >> beam.Map(trata_data)
    | "Criando chave estado" >> beam.Map(chave_uf)
    | "Agrupando por estado" >> beam.GroupByKey() #agrupa pela chave que nesse caso é o UF
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) #para o retorno de um yield usar FlatMap
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum) 
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()