import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options= PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


colunas_dengue =[
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

def texto_para_lista(elemento, delimitador="|"):
    """
    Recebe o texto e transforma em lista
    """
    return elemento.split(delimitador)

def lista_para_dicionario(elemento, colunas):
    """
    Recebe lista e transforma para dicionário possibilitando a inserção do cabeçalho como identificadores
    """
    return dict(zip(colunas,elemento))

def trata_data(elemento):
    """
    Inserindo um novo campo ano_mes no dicionário
    """
    elemento['ano_mes']= '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Criando uma chave baseada no UF
    """
    chave= elemento['uf']
    return(chave,elemento)

def casos_dengue(elemento):
    """
    Retornando um yield e tratando os casos nulos.
    Ex de retorno: SC-2020-08
    """
    uf,registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}",float(registro['casos']))
        else:
            yield(f"{uf}-{registro['ano_mes']}", 0.0)


dengue=(
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('./data/casos_dengue.txt', skip_header_lines=1) 
    | "De texto para lista" >> beam.Map(texto_para_lista) 
    | "Lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ANO_MES" >> beam.Map(trata_data)
    | "Criando chave estado" >> beam.Map(chave_uf)
    | "Agrupando por estado" >> beam.GroupByKey() 
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) 
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum) 
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()