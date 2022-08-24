import enum
import re
from unittest import skipIf
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
    Ex de retorno: 'SC-2020-08', 00.0
    """
    uf,registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}",float(registro['casos']))
        else:
            yield(f"{uf}-{registro['ano_mes']}", 0.0)

def criando_chave(elemento):
    """
    Criando a chave do dataset Chuvas no formato UF-ANO-MES e convertendo os números negativos em 0
    Ex de retorno ('SP-2020-08', 900)
    """
    data,mm,uf = elemento
    ano_mes= '-'.join(data.split('-')[:2])
    chaveChuva= f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm= 0.0
    else:
        mm = float(mm)
    return chaveChuva, float(mm)

def arredonda_mm(elemento):
    """
    Arredondar os mms resultantes do Combine 
    De: ('RO-2019-08', 66.20000000000003)
    Para: ('RO-2019-08', 66.2)
    """
    chave,mm = elemento
    return(chave,round(mm, 1))

colun= [
    'id',
    'informacoes',
]


dengue=(
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('sample_casos_dengue.txt', skip_header_lines=1) 
    | "De texto para lista" >> beam.Map(texto_para_lista) 
    | "Lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criando campo ANO_MES" >> beam.Map(trata_data)
    | "Criando chave UF" >> beam.Map(chave_uf)
    | "Agrupando por UF" >> beam.GroupByKey() #agrupando pela key UF
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) 
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum) #somando os casos com keys iguais
    #| "Mostrando resultados de dengue" >> beam.Map(print) 
)

chuvas=(
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('sample_chuvas.csv',skip_header_lines=1)
    | "texto para lista" >> beam.Map(texto_para_lista, delimitador=',')
    | "Acrescentando chave UF-ANO-MES" >> beam.Map(criando_chave)
    | "Somando mm com chaves iguais" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >>beam.Map(arredonda_mm)
    #| "Mostrando resultado de chuvas" >> beam.Map(print)
)

resultado =(
    (chuvas,dengue)
    | "Empilhando as collections" >> beam.Flatten()
    | "Agrupando as collections" >> beam.GroupByKey()
    | "Mostrando da união das collections" >> beam.Map(print)
    
)
pipeline.run()