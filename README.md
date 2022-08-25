### Apache Beam -> Data Pipeline com Python - Índice de Dengue vs Quantia de Chuva -- UF/MES/ANO
Projeto realizado com a finalidade de entender e praticar processos da engenharia de dados.
Inicialmente temos dois datasets:

    1. Casos de dengue -> Versão full (/data/casos_dengue.txt) e versão sample (sample_casos_dengue.txt):
    Exemplo da estrutura inicial:
        id|data_iniSE|casos|ibge_code|cidade|uf|cep|latitude|longitude
        0|2015-11-08|0.0|230010|Abaiara|CE|63240-000|-7.3364|-39.0613  


    2. Quantia de chuva em MM -> Versão full (/data/chuvas.csv) e versão sample (sample_chuvas.csv):
    Exemplo da estrutura inicial:
        data,mm,uf
        2015-01-01,0.0,CE

Foi realizado o carregamento, limpeza e tratamento de campos/dados, junção/relacionamento das bases a partir de chaves definidas durante o processo (resumidamente UF-ANO-MES) e por fim a geração do arquivo com o resultado da pipeline em CSV (resultado-00000-of-00001.csv).

    Exemplo da estrutura do resultado final:
    UF;ANO;MES;CHUVA;DENGUE
    SP;2015;01;4465.0;772.0
