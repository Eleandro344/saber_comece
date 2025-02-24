from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta

# Create your views here.
@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticado
def home_view(request):
    return render(request, 'index.html')

# apps/views.py

# apps/views.py

from django.shortcuts import render
import pandas as pd
import mysql.connector


def carregar_dados_remessa_itau(codigo_doc_itau=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
            SELECT 
                `DATA DE GERAÇÃO HEADER`,
                `CÓD. DE OCORRÊNCIA`,
                `CODIGO DO DOC`,
                `DATA DE EMISSÃO`,
                `VENCIMENTO`,
                `NOME`,
                `DESCONTO ATÉ`,
                `INSTRUÇÃO 1`,
                `INSTRUÇÃO 2`,
                `VALOR DO DESCONTO`,
                `VALOR DO IOF`,
                `VALOR DO TÍTULO`
            FROM remessa_itau
        """
        if codigo_doc_itau:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessaunicred_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_itau])
            
            novos_nomes = {
            'DATA DE GERAÇÃO HEADER': 'Data da Ocorrencia',
            'CÓD. DE OCORRÊNCIA': 'Ocorrencia',
            # ... adicione os outros nomes conforme necessário
            }
            remessaunicred_bd.rename(columns=novos_nomes, inplace=True)
            remessaunicred_bd = remessaunicred_bd.sort_values(by='Data da Ocorrencia', ascending=True)
            remessaunicred_bd['Data da Ocorrencia'] = pd.to_datetime(remessaunicred_bd['Data da Ocorrencia'])

            remessaunicred_bd = remessaunicred_bd.sort_values(by=['Data da Ocorrencia'], ascending=[True])
            remessaunicred_bd.loc[remessaunicred_bd['INSTRUÇÃO 1'] == 0, 'INSTRUÇÃO 1'] = "Não protestar"
            remessaunicred_bd.loc[remessaunicred_bd['INSTRUÇÃO 1'] != 'Não protestar', 'INSTRUÇÃO 1'] = "Protesto automatico"
    
            remessaunicred_bd.loc[remessaunicred_bd['Ocorrencia'] == 'REMESSA DE TÍTULOS', 'Ocorrencia'] = "ENVIADO"
        else:
            remessaunicred_bd = pd.read_sql(consulta_remessa, con=mydb)
            remessaunicred_bd = remessaunicred_bd.head(5)
            novos_nomes = {
            'DATA DE GERAÇÃO HEADER': 'Data da Ocorrencia',
            'CÓD. DE OCORRÊNCIA': 'Ocorrencia',
            # ... adicione os outros nomes conforme necessário
            }
            remessaunicred_bd.rename(columns=novos_nomes, inplace=True)
            remessaunicred_bd = remessaunicred_bd.sort_values(by='Data da Ocorrencia', ascending=True)
            remessaunicred_bd['Data da Ocorrencia'] = pd.to_datetime(remessaunicred_bd['Data da Ocorrencia'])

            remessaunicred_bd = remessaunicred_bd.sort_values(by=['Data da Ocorrencia'], ascending=[True])
            remessaunicred_bd.loc[remessaunicred_bd['INSTRUÇÃO 1'] == 0, 'INSTRUÇÃO 1'] = "Não protestar"
            remessaunicred_bd.loc[remessaunicred_bd['INSTRUÇÃO 1'] != 'Não protestar', 'INSTRUÇÃO 1'] = "Protesto automatico"
    
            remessaunicred_bd.loc[remessaunicred_bd['Ocorrencia'] == 'REMESSA DE TÍTULOS', 'Ocorrencia'] = "ENVIADO"        

            mydb.close()

        return remessaunicred_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()




def carregar_dados_retorno_retorno_itau(codigo_doc_itau=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
    SELECT 
            `DATA DE GERAÇÃO HEADER`,
            `CÓD. DE OCORRÊNCIA`, 
            `CODIGO DO DOC`, 
            `VALOR DO TÍTULO`,
            `JUROS DE MORA/MULTA`,
            `VALOR PRINCIPAL`,
            `VALOR DO IOF`,
            `VENCIMENTO`, 
            `DESCONTOS`, 
            `ERROS`,
            `CÓD. DE LIQUIDAÇÃO`, 
            `NÚMERO SEQUENCIAL`,
            `NOME DO BANCO HEADER`,
            `DATA DE GERAÇÃO HEADER`,
            `NUMERO SEQUENCIAL HEADER`, 
            `NÚMERO SEQUENCIAL TRAILER`
    FROM retorno_itau
"""
        if codigo_doc_itau:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            retornounicred_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_itau])
            mydb.close()
    # Reorder the columns
            # retornounicred_bd = retornounicred_bd[new_column_order]        
            retornounicred_bd = retornounicred_bd.loc[:, ~retornounicred_bd.columns.duplicated()]

            # # Ordenar o DataFrame pela coluna 'Data da Ocorrência' de forma crescente
        #  df_retorno = df_retorno.sort_values(by='DATA DE GERAÇÃO HEADER')
                


            novos_nomes = {
                'DATA DE GERAÇÃO HEADER': 'Data da Ocorrencia',
                'CÓD. DE OCORRÊNCIA': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            retornounicred_bd.rename(columns=novos_nomes, inplace=True)
            retornounicred_bd.dropna(subset=['Data da Ocorrencia'], inplace=True)

            retornounicred_bd['Data da Ocorrencia'] = pd.to_datetime(retornounicred_bd['Data da Ocorrencia'], format='%d%m%y')

            retornounicred_bd = retornounicred_bd.sort_values(by='Data da Ocorrencia', ascending=True)
            retornounicred_bd['Data da Ocorrencia'] = pd.to_datetime(retornounicred_bd['Data da Ocorrencia'])

        # retornounicred_bd = retornounicred_bd.sort_values(by=['Data da Ocorrencia', 'NÚMERO SEQUENCIAL'], ascending=[True, True])
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'BAIXA COM TRANSFERÊNCIA PARA D', 'Ocorrencia'] = "BAIXA COM TRANSFERÊNCIA PARA DESCONTO"
        else:
            retornounicred_bd = pd.read_sql(consulta_remessa, con=mydb)
            retornounicred_bd = retornounicred_bd.head(5)
            # retornounicred_bd = retornounicred_bd[new_column_order]        
            retornounicred_bd = retornounicred_bd.loc[:, ~retornounicred_bd.columns.duplicated()]

            novos_nomes = {
                'DATA DE GERAÇÃO HEADER': 'Data da Ocorrencia',
                'CÓD. DE OCORRÊNCIA': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            retornounicred_bd.rename(columns=novos_nomes, inplace=True)
            retornounicred_bd.dropna(subset=['Data da Ocorrencia'], inplace=True)

            retornounicred_bd['Data da Ocorrencia'] = pd.to_datetime(retornounicred_bd['Data da Ocorrencia'], format='%d%m%y')

            retornounicred_bd = retornounicred_bd.sort_values(by='Data da Ocorrencia', ascending=True)
            retornounicred_bd['Data da Ocorrencia'] = pd.to_datetime(retornounicred_bd['Data da Ocorrencia'])

        # retornounicred_bd = retornounicred_bd.sort_values(by=['Data da Ocorrencia', 'NÚMERO SEQUENCIAL'], ascending=[True, True])
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'BAIXA COM TRANSFERÊNCIA PARA D', 'Ocorrencia'] = "BAIXA COM TRANSFERÊNCIA PARA DESCONTO"            
        return retornounicred_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()




def carregar_dados_remessa(codigo_doc=None):
    try:
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',
            database='db_sabertrimed',
        )
        consulta_remessa = """
            SELECT 
                `Data da Gravação do Arquivo`,
                `Identificação da Ocorrência`,
                `CODIGO DO DOC`,
                `Data de emissão do Título`,
                `Data de vencimento do Título`,
                `Valor do Título`,
                `Nome/Razão Social do Pagador`,
                `Data Limite P/Concessão de Desconto`,
                `Valor do Desconto`,
                `Nº Sequencial do Registro remessa`,
                `Nº Sequencial do Arquivo`,
                `Nº Sequencial do Registro`
            FROM unicredremessa
        """
        if codigo_doc:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessaunicred_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc])
            remessaunicred_bd = remessaunicred_bd.sort_values(by='Data da Gravação do Arquivo', ascending=True)

            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Identificação da Ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessaunicred_bd.rename(columns=novos_nomes, inplace=True)
            remessaunicred_bd['Data da Ocorrencia'] = pd.to_datetime(remessaunicred_bd['Data da Ocorrencia'], format='%d%m%y', errors='coerce')

            # Formatar a coluna 'Data da Ocorrencia' no novo formato desejado
            remessaunicred_bd['Data da Ocorrencia'] = remessaunicred_bd['Data da Ocorrencia'].dt.strftime('%d/%m/%Y')                
        else:
            remessaunicred_bd = pd.read_sql(consulta_remessa, con=mydb)
            remessaunicred_bd =  remessaunicred_bd.head(5)            

            remessaunicred_bd = remessaunicred_bd.sort_values(by='Data da Gravação do Arquivo', ascending=True)

            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Identificação da Ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessaunicred_bd.rename(columns=novos_nomes, inplace=True)
            remessaunicred_bd['Data da Ocorrencia'] = pd.to_datetime(remessaunicred_bd['Data da Ocorrencia'], format='%d%m%y', errors='coerce')

            # Formatar a coluna 'Data da Ocorrencia' no novo formato desejado
            remessaunicred_bd['Data da Ocorrencia'] = remessaunicred_bd['Data da Ocorrencia'].dt.strftime('%d/%m/%Y')    

            mydb.close()

        return remessaunicred_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()


def carregar_dados_retorno(codigo_doc=None):
    try:
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',
            database='db_sabertrimed',
        )
        consulta_retorno = """
            SELECT 
                `DATA DA GERAÇÃO DO ARQUIVO`,
                `TIPO DE INSTRUÇÃO ORIGEM`,
                `CÓDIGO DE MOVIMENTO`,
                `CODIGO DO DOC`,
                `COMPLEMENTO DO MOVIMENTO`,
                `DATA LIQUIDAÇÃO`,
                `CANAL DE LIQUIDAÇÃO`,
                `VALOR DO TÍTULO`,
                `VALOR ABATIMENTO`,
                `VALOR PAGO`,
                `JUROS DE MORA`,
                `VALOR LÍQUIDO`,
                `DATA DE VENCIMENTO`,
                `DATA PROGRAMADA PARA REPASSE`,
                `VALOR DA TARIFA`,
                `DATA DE DEBITO DA TARIFA`,
                `VALOR DESCONTO CONCEDIDO`,
                `SEQUENCIAL DO REGISTRO`,
                `NOME DO BENEFICIÁRIO`,
                `SEQUENCIAL DO RETORNO`
            FROM retornounicred
        """
        if codigo_doc:
            consulta_retorno += " WHERE `CODIGO DO DOC` = %s"
            retornounicred_bd = pd.read_sql(consulta_retorno, con=mydb, params=[codigo_doc])
            retornounicred_bd['DATA DA GERAÇÃO DO ARQUIVO'] = pd.to_datetime(retornounicred_bd['DATA DA GERAÇÃO DO ARQUIVO'])

            # Ordenar o DataFrame pela coluna 'Data da Ocorrência' de forma crescente
            #df_retorno = df_retorno.sort_values(by='DATA DA GERAÇÃO DO ARQUIVO')
            retornounicred_bd = retornounicred_bd.sort_values(by=['DATA DA GERAÇÃO DO ARQUIVO', 'SEQUENCIAL DO REGISTRO'], ascending=[True, True])
                
            novos_nomes = {
                'DATA DA GERAÇÃO DO ARQUIVO': 'Data da Ocorrencia',
                'CÓDIGO DE MOVIMENTO': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            retornounicred_bd.rename(columns=novos_nomes, inplace=True)
            #df_retorno.loc[df_retorno['Ocorrencia'] == 'Protesto solicitado', 'Ocorrencia'] = "Enviado a Cartorio"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Protesto solicitado', 'Ocorrencia'] = "Enviado a Cartorio"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Título Descontável (título com desistênc', 'Ocorrencia'] = "Devolvido"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada', 'Ocorrencia'] = "Instrução Confirmada"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Liquidação de Título Descontado', 'Ocorrencia'] = "Liquidação de Título Trocado"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Título Descontado', 'Ocorrencia'] = "Trocado"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Pago(Título protestado pago em cartório)', 'Ocorrencia'] = "Pago em Cartório"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Pedido de ', 'Ocorrencia'] = "Solicitação de Baixa"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Protesto Em cartório', 'Ocorrencia'] = "Em cartório"
            #df_remessa.loc[df_remessa['TIPO DE INSTRUÇÃO ORIGEM'] == 'Protesto Em cartório', 'TIPO DE INSTRUÇÃO ORIGEM'] = "Em cartório"

            condicao = (retornounicred_bd['TIPO DE INSTRUÇÃO ORIGEM'] == 'Protestar') & (retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada')

            # Atualizar o valor da coluna "Ocorrencia" para "boleto protestado" onde a condição for verdadeira
            retornounicred_bd.loc[condicao, 'Ocorrencia'] = 'Boleto Protestado'
            retornounicred_bd['DATA LIQUIDAÇÃO'] = pd.to_datetime(retornounicred_bd['DATA LIQUIDAÇÃO'], format='%d%m%y', errors='coerce')


            condicao = (retornounicred_bd['TIPO DE INSTRUÇÃO ORIGEM'] == ' Pedido de Baixa') & (retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada')

            # Atualizar o valor da coluna "Ocorrencia" para "boleto protestado" onde a condição for verdadeira
            retornounicred_bd.loc[condicao, 'Ocorrencia'] = 'Boleto Baixado'
            # Formatar a coluna 'Data da Ocorrencia' no novo formato desejado
            retornounicred_bd['DATA LIQUIDAÇÃO'] = retornounicred_bd['DATA LIQUIDAÇÃO'].dt.strftime('%d/%m/%Y')
        else:
            retornounicred_bd = pd.read_sql(consulta_retorno, con=mydb)
            retornounicred_bd = retornounicred_bd.head(5)

            retornounicred_bd['DATA DA GERAÇÃO DO ARQUIVO'] = pd.to_datetime(retornounicred_bd['DATA DA GERAÇÃO DO ARQUIVO'])

            # Ordenar o DataFrame pela coluna 'Data da Ocorrência' de forma crescente
            #df_retorno = df_retorno.sort_values(by='DATA DA GERAÇÃO DO ARQUIVO')
            retornounicred_bd = retornounicred_bd.sort_values(by=['DATA DA GERAÇÃO DO ARQUIVO', 'SEQUENCIAL DO REGISTRO'], ascending=[True, True])
                
            novos_nomes = {
                'DATA DA GERAÇÃO DO ARQUIVO': 'Data da Ocorrencia',
                'CÓDIGO DE MOVIMENTO': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            retornounicred_bd.rename(columns=novos_nomes, inplace=True)
            #df_retorno.loc[df_retorno['Ocorrencia'] == 'Protesto solicitado', 'Ocorrencia'] = "Enviado a Cartorio"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Protesto solicitado', 'Ocorrencia'] = "Enviado a Cartorio"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Título Descontável (título com desistênc', 'Ocorrencia'] = "Devolvido"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada', 'Ocorrencia'] = "Instrução Confirmada"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Liquidação de Título Descontado', 'Ocorrencia'] = "Liquidação de Título Trocado"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Título Descontado', 'Ocorrencia'] = "Trocado"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Pago(Título protestado pago em cartório)', 'Ocorrencia'] = "Pago em Cartório"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Pedido de ', 'Ocorrencia'] = "Solicitação de Baixa"
            retornounicred_bd.loc[retornounicred_bd['Ocorrencia'] == 'Protesto Em cartório', 'Ocorrencia'] = "Em cartório"
            #df_remessa.loc[df_remessa['TIPO DE INSTRUÇÃO ORIGEM'] == 'Protesto Em cartório', 'TIPO DE INSTRUÇÃO ORIGEM'] = "Em cartório"

            condicao = (retornounicred_bd['TIPO DE INSTRUÇÃO ORIGEM'] == 'Protestar') & (retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada')

            # Atualizar o valor da coluna "Ocorrencia" para "boleto protestado" onde a condição for verdadeira
            retornounicred_bd.loc[condicao, 'Ocorrencia'] = 'Boleto Protestado'
            retornounicred_bd['DATA LIQUIDAÇÃO'] = pd.to_datetime(retornounicred_bd['DATA LIQUIDAÇÃO'], format='%d%m%y', errors='coerce')


            condicao = (retornounicred_bd['TIPO DE INSTRUÇÃO ORIGEM'] == ' Pedido de Baixa') & (retornounicred_bd['Ocorrencia'] == 'Instrução Confirmada')

            # Atualizar o valor da coluna "Ocorrencia" para "boleto protestado" onde a condição for verdadeira
            retornounicred_bd.loc[condicao, 'Ocorrencia'] = 'Boleto Baixado'
            # Formatar a coluna 'Data da Ocorrencia' no novo formato desejado
            retornounicred_bd['DATA LIQUIDAÇÃO'] = retornounicred_bd['DATA LIQUIDAÇÃO'].dt.strftime('%d/%m/%Y')            

            mydb.close()

        return retornounicred_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()






@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def unicred_view(request):
    codigo_doc = request.GET.get('codigo_doc')
    remessa_data = carregar_dados_remessa(codigo_doc)
    retorno_data = carregar_dados_retorno(codigo_doc)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc': codigo_doc,
    }

    return render(request, 'unicred.html', context)


# FATURAMENTO

from django.shortcuts import render
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
def carregar_dados_faturamento():
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',
            database='db_sabertrimed',
        )

        # Consultas de remessas
        remessa_santander = pd.read_sql("SELECT * FROM remessa_santander", con=mydb)
        rememessa_uncired = pd.read_sql("SELECT * FROM unicredremessa", con=mydb)
        remessa_safra = pd.read_sql("SELECT * FROM remessa_safra", con=mydb)
        remessa_sofisa = pd.read_sql("SELECT * FROM remessa_sofisa", con=mydb)
        remessa_itau = pd.read_sql("SELECT * FROM remessa_itau", con=mydb)
        remessa_sicoob = pd.read_sql("SELECT * FROM remessa_sicoob", con=mydb)
        remessa_grafeno = pd.read_sql("SELECT * FROM remessa_grafeno", con=mydb)

        # Data de ontem
        hoje = datetime.now()
        if hoje.weekday() == 0:
            ontem = hoje - timedelta(days=3)
        else:
            ontem = hoje - timedelta(days=1)
        ontem = ontem.strftime('%Y-%m-%d')

        # Processamento das remessas
# Processamento das remessas
        santander = remessa_santander[remessa_santander['Código de movimento remessa'] == 'Enviado']
        santander = santander[santander['Nº de inscrição do beneficiário'] != 0]
        santander['Valor nominal do boleto'] = santander['Valor nominal do boleto'].str.replace(',', '.').astype(float)
        santander['Data de emissão do boleto'] = pd.to_datetime(santander['Data de emissão do boleto'], format='%d/%m/%y', errors='coerce')
        santander = santander[santander['Data de emissão do boleto'] == ontem]
        santandertotal = santander['Valor nominal do boleto'].sum()

        remessa_itau = remessa_itau[["DATA DE GERAÇÃO HEADER","DATA DE EMISSÃO","CÓD. DE OCORRÊNCIA", "NOME DO BANCO HEADER","VALOR DO TÍTULO"]]
        remessa_itau['DATA DE EMISSÃO'] = pd.to_datetime(remessa_itau['DATA DE EMISSÃO'], format='%d/%m/%y')
        remessa_itau= remessa_itau[remessa_itau['CÓD. DE OCORRÊNCIA'] =='REMESSA DE TÍTULOS']   
        itauontem = remessa_itau[remessa_itau['DATA DE EMISSÃO'] == ontem]
        itauontem['VALOR DO TÍTULO'] = itauontem['VALOR DO TÍTULO'].str.replace(',', '.').astype(float)

        itautotal = itauontem['VALOR DO TÍTULO'].sum()


        sicoob = remessa_sicoob[remessa_sicoob['Ocorrencia'] == 'Enviado']
        sicoob['Data Emissão do Título'] = pd.to_datetime(sicoob['Data Emissão do Título'], format='%d%m%Y', errors='coerce')
        sicoobontem = sicoob[sicoob['Data Emissão do Título'] == ontem]
        sicoobontem['Valor do Título'] = sicoobontem['Valor do Título'].str.replace(',', '.').astype(float)
        sicoobtotal = sicoobontem['Valor do Título'].sum()

        unicred = rememessa_uncired[rememessa_uncired['Identificação da Ocorrência'] == 'Enviado']
        unicred['Data de emissão do Título'] = pd.to_datetime(unicred['Data de emissão do Título'], format='%d/%m/%y', errors='coerce')
        unicred = unicred[unicred['Data de emissão do Título'] == ontem]
        unicred['Valor do Título'] = unicred['Valor do Título'].str.replace(',', '.').astype(float)
        totalunicred = unicred['Valor do Título'].sum()


        grafeno = remessa_grafeno[["Data de gravação do arquivo cabeçalho","Identificação da ocorrência", "Banco","Valor"]]
        grafeno['Banco'] = grafeno['Banco'].astype(str)
        grafeno.loc[:, 'Banco'] = 'GRAFENO'



        grafeno = grafeno[remessa_grafeno['Identificação da ocorrência'] == 'Enviado']
        grafeno['Valor'] = grafeno['Valor'].str.replace(',', '.').astype(float)
        grafeno['Data de gravação do arquivo cabeçalho'] = pd.to_datetime(grafeno['Data de gravação do arquivo cabeçalho'], format='%d/%m/%y', errors='coerce')
        grafeno = grafeno[grafeno['Data de gravação do arquivo cabeçalho'] == ontem]
        grafenototal = grafeno['Valor'].sum()




        safra = remessa_safra[remessa_safra['Cod. Ocorrência'] == 'REMESSA DE TÍTULOS']
        safra['Nome do Banco'] = "SAFRA"        
        safra['Data De Emissão Do Título'] = pd.to_datetime(safra['Data De Emissão Do Título'], format='%d/%m/%y', errors='coerce')
        safra = safra[safra['Data De Emissão Do Título'] == ontem]
        safra['Valor Do Título'] = safra['Valor Do Título'].str.replace(',', '.').astype(float)
        safratotal = safra['Valor Do Título'].sum()

        sofisa = remessa_sofisa[remessa_sofisa['Código de movimento remessa'] == 'Enviado']
        sofisa['Data de emissão do boleto'] = pd.to_datetime(sofisa['Data de emissão do boleto'], format='%d/%m/%y', errors='coerce')
        sofisa = sofisa[sofisa['Data de emissão do boleto'] == ontem]
        sofisa['Valor nominal do boleto'] = sofisa['Valor nominal do boleto'].str.replace(',', '.').astype(float)
        total_sofisa = sofisa['Valor nominal do boleto'].sum()


        # Criação da tabela completa
        grafeno = grafeno.rename(columns={'Data de gravação do arquivo cabeçalho': 'Emissao Doc', 'Valor': 'Valor', 'Identificação da ocorrência': 'Ocorrencia', 'Banco': 'Nome do Banco'})
        santander = santander.rename(columns={'Data de emissão do boleto': 'Emissao Doc', 'Valor nominal do boleto': 'Valor', 'Código de movimento remessa': 'Ocorrencia', 'Nome do Banco': 'Nome do Banco'})
        itauontem = itauontem.rename(columns={'DATA DE EMISSÃO': 'Emissao Doc', 'VALOR DO TÍTULO': 'Valor', 'CÓD. DE OCORRÊNCIA': 'Ocorrencia', 'NOME DO BANCO HEADER': 'Nome do Banco'})
        sicoobontem = sicoobontem.rename(columns={'Data Emissão do Título': 'Emissao Doc', 'Valor do Título': 'Valor', 'Ocorrencia': 'Ocorrencia', 'Nome do Banco': 'Nome do Banco'})
        unicred = unicred.rename(columns={'Data de emissão do Título': 'Emissao Doc', 'Valor do Título': 'Valor', 'Identificação da Ocorrência': 'Ocorrencia', 'Nome do Banco por Extenso': 'Nome do Banco'})
        safra = safra.rename(columns={'Data De Emissão Do Título': 'Emissao Doc', 'Valor Do Título': 'Valor', 'Cod. Ocorrência': 'Ocorrencia', 'Nome do Banco': 'Nome do Banco'})
        sofisa = sofisa.rename(columns={'Data de emissão do boleto': 'Emissao Doc', 'Valor nominal do boleto': 'Valor', 'Código de movimento remessa': 'Ocorrencia', 'Nome do Banco': 'Nome do Banco'})

        tabelacompleta = pd.concat([grafeno,santander, itauontem, sicoobontem, unicred, safra, sofisa], ignore_index=True)
        tabelacompleta['Quantidade de Titulos'] = 1

        total_por_banco = tabelacompleta.groupby(['Emissao Doc', 'Nome do Banco'])[['Quantidade de Titulos', 'Valor']].sum().reset_index()
        total_por_banco = total_por_banco.rename(columns={'Valor': 'Total'})

        total_por_banco['Emissao Doc'] = pd.to_datetime(total_por_banco['Emissao Doc'])
        
        total_por_banco['Emissao Doc'] = total_por_banco['Emissao Doc'].dt.strftime('%d/%m/%Y')
        grafico = total_por_banco
        grafico = grafico.replace("R$", "").replace(".", "").replace(",", ".")




        total_por_banco['Total'] = total_por_banco['Total'].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        df_vazio = total_por_banco['Quantidade de Titulos'].astype(int)
        if df_vazio.empty:
            exibicao = "nao"
        else:
            exibicao = "sim"
        itautotal = f'{itautotal:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        grafenototal = f'{grafenototal:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        santandertotal = f'{santandertotal:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        totalunicred = f'{totalunicred:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        safratotal = f'{safratotal:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        total_sofisa = f'{total_sofisa:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        sicoobtotal = f'{sicoobtotal:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")

        return total_por_banco, exibicao,grafenototal,santandertotal, grafico,totalunicred, safratotal, total_sofisa, itautotal, sicoobtotal
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return None, None
    
   

@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad  # Redireciona para a página de login se o usuário não estiver autenticado
def faturamento_view(request):
    # Chama a função para carregar os dados de faturamento
    total_por_banco, exibicao, grafenototal, santandertotal, grafico, totalunicred, safratotal, total_sofisa, itautotal, sicoobtotal = carregar_dados_faturamento()

    # Passa as variáveis para o template
    context = {
        'total_por_banco': total_por_banco.to_dict(orient='records'),
        'exibicao': exibicao,
        'grafenototal': grafenototal,
        'santandertotal': santandertotal,
        'grafico': grafico.to_dict(orient='records'),
        'totalunicred': totalunicred,
        'safratotal': safratotal,
        'total_sofisa': total_sofisa,
        'itautotal': itautotal,
        'sicoobtotal': sicoobtotal
    }
    return render(request, 'faturamento.html', context)


   

@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad  # Redireciona para a página de login se o usuário não estiver autenticado
def itau_view(request):
    codigo_doc_itau = request.GET.get('codigo_doc_itau')
    remessa_data = carregar_dados_remessa_itau(codigo_doc_itau)
    retorno_data = carregar_dados_retorno_retorno_itau(codigo_doc_itau)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc_itau': codigo_doc_itau,
    }

    return render(request, 'itau.html', context)


# GRAFENO




# Função para carregar os dados da tabela de remessa
def carregar_dados_remessa_grafeno(codigo_doc=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
            SELECT
                `Data de gravação do arquivo cabeçalho`,
                `Identificação da ocorrência`,
                `CODIGO DO DOC`,
                `Data da emissão do título` ,
                `Cliente`,
                `Data do vencimento`,
                `desconto ate`,
                `Desconto`,
                `Identificação da empresa beneficiária no banco`,
                `Número de controle do participante`,
                `Código do banco a ser debitado na câmara de compensação`,
                `Campo de multa`,
                `Percentual multa`,
                `Identificação do título no banco`,
                `Dígito de auto conferência do número bancário`,
                `Quantidade de pagamentos possíveis`,
                `Valor`,	
                `Banco`,
                `Agência depositária`,
                `Identificação`,
                `Valor a ser cobrado por dia de atraso`,
                `Valor do desconto`,
                `Valor do IOF`,
                `Valor do abatimento a ser concedido ou cancelado`,
                `cnpj pagador`,
                `endereco`,
                `Número sequencial do registro`,
                `Nome do arquivo`,
                `TIPO DE REGISTRO HEADER cabeçalho`,
                `Identificação do arquivo remessa cabeçalho`,
                `Literal remessa cabeçalho`,
                `Código do serviço cabeçalho`,
                `Nome da empresa cabeçalho`,
                `Número do Banco cabeçalho`,
                `Nome do banco cabeçalho`,
                `Identificação do sistema cabeçalho`,
                `Número sequencial de remessa cabeçalho`,
                `Número sequencial do registro cabeçalho`,
                `Identificação do registro trailer`,
                `Número sequencial de registro trailer`,
                `Identificação do registro`
            FROM `remessa_grafeno`            
            """
        if codigo_doc:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessa_grafeno = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc])
            remessa_grafeno = remessa_grafeno.sort_values(by='Data de gravação do arquivo cabeçalho', ascending=True)

            novos_nomes = {
                'Data de gravação do arquivo cabeçalho': 'Data da Ocorrencia',
                'Identificação da ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessa_grafeno.rename(columns=novos_nomes, inplace=True)
            remessa_grafeno['Data da Ocorrencia'] = pd.to_datetime(remessa_grafeno['Data da Ocorrencia'])
            remessa_grafeno = remessa_grafeno.sort_values(by='Data da Ocorrencia', ascending=True)       
        else:
            remessa_grafeno = pd.read_sql(consulta_remessa, con=mydb)
            remessa_grafeno = remessa_grafeno.head(10)
            remessa_grafeno = remessa_grafeno.sort_values(by='Data de gravação do arquivo cabeçalho', ascending=True)

            novos_nomes = {
                'Data de gravação do arquivo cabeçalho': 'Data da Ocorrencia',
                'Identificação da ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessa_grafeno.rename(columns=novos_nomes, inplace=True)
            remessa_grafeno['Data da Ocorrencia'] = pd.to_datetime(remessa_grafeno['Data da Ocorrencia'])
            remessa_grafeno = remessa_grafeno.sort_values(by='Data da Ocorrencia', ascending=True)    


        mydb.close()

        return remessa_grafeno
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()
    

def carregar_dados_retorno_grafeno(codigo_doc=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT
            `Data de gravação do arquivo cabeçalho`,
            `Identificação da ocorrência`,
            `CODIGO DO DOC`,
            `Valor do título`,
            `Data de vencimento do título`,
            `Valor pago`,
            `Juros de mora`,            
            `Número de inscrição da empresa`,
            `Identificação da empresa beneficiária no banco`,
            `Número de controle do participante`,
            `Identificação do título no banco`,
            `Data do ocorrência no banco`,
            `Banco cobrador`,
            `Agência Cobrança`,	
            `Despesas de cobrança`,	
            `Valor abatimento concedido`,
            `Outros créditos`,
            `Data do crédito`,
            `Origem do pagamento`,
            `Motivos de rejeição`,
            `Número sequencial de registro`,
            `Nome do arquivo`,
            `TIPO DE REGISTRO HEADER cabeçalho`,
            `Identificação do arquivo remessa cabeçalho`,
            `Literal remessa cabeçalho`,
            `Código do serviço cabeçalho`,
            `Nome da empresa cabeçalho`,
            `Número do Banco cabeçalho`,
            `Nome do banco cabeçalho`,
            `Identificação do sistema cabeçalho`,
            `Número sequencial de remessa cabeçalho`,
            `Número sequencial do registro cabeçalho`,
            `Identificação do registro trailer`,
            `Número sequencial de registro trailer`,
            `Identificação do registro`
            FROM `retorno_grafeno`            
        """
        if codigo_doc:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            retornosantander_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc])
            retornosantander_bd = retornosantander_bd.sort_values(by='Data de gravação do arquivo cabeçalho', ascending=True)
            novos_nomes = {
                'Data de gravação do arquivo cabeçalho': 'Data da Ocorrencia',

                'Identificação da ocorrência': 'Ocorrencia',
            }
            retornosantander_bd.rename(columns=novos_nomes, inplace=True)


            docsativos = retornosantander_bd[['Data da Ocorrencia','Nome da empresa cabeçalho','Ocorrencia','Valor do título','CODIGO DO DOC','Valor pago',]]
            docsativos['Data da Ocorrencia'] = pd.to_datetime(docsativos['Data da Ocorrencia'])

            # Primeiro, vamos ordenar o DataFrame pela coluna 'DATA DA GERAÇÃO DO ARQUIVO' para garantir que a data mais recente apareça primeiro
            df_sorted = docsativos.sort_values(by='Data da Ocorrencia', ascending=False)

            # Em seguida, vamos identificar os índices dos registros com a data mais recente para cada número de documento
            indices_mais_recentes = df_sorted.groupby('CODIGO DO DOC')['Data da Ocorrencia'].idxmax()

            # Agora, vamos usar esses índices para selecionar os registros correspondentes
            docsativos = df_sorted.loc[indices_mais_recentes]

            # Exibindo os documentos mais recentes


            naopago = docsativos.loc[docsativos['Valor pago'] == 0.0]
            baixados = naopago.loc[naopago['Ocorrencia'] == 'Baixado co']  
            naopago = naopago.loc[naopago['Ocorrencia'] != 'Baixado co']  

            pagos = docsativos.loc[docsativos['Valor pago'] != 0.0]

        else:
            retornosantander_bd = pd.read_sql(consulta_remessa, con=mydb)
            retornosantander_bd = retornosantander_bd.head(10)
            retornosantander_bd = retornosantander_bd.sort_values(by='Data de gravação do arquivo cabeçalho', ascending=True)
            novos_nomes = {
                'Data de gravação do arquivo cabeçalho': 'Data da Ocorrencia',

                'Identificação da ocorrência': 'Ocorrencia',
            }
            retornosantander_bd.rename(columns=novos_nomes, inplace=True)


            docsativos = retornosantander_bd[['Data da Ocorrencia','Nome da empresa cabeçalho','Ocorrencia','Valor do título','CODIGO DO DOC','Valor pago',]]
            docsativos['Data da Ocorrencia'] = pd.to_datetime(docsativos['Data da Ocorrencia'])

            # Primeiro, vamos ordenar o DataFrame pela coluna 'DATA DA GERAÇÃO DO ARQUIVO' para garantir que a data mais recente apareça primeiro
            df_sorted = docsativos.sort_values(by='Data da Ocorrencia', ascending=False)

            # Em seguida, vamos identificar os índices dos registros com a data mais recente para cada número de documento
            indices_mais_recentes = df_sorted.groupby('CODIGO DO DOC')['Data da Ocorrencia'].idxmax()

            # Agora, vamos usar esses índices para selecionar os registros correspondentes
            docsativos = df_sorted.loc[indices_mais_recentes]

            # Exibindo os documentos mais recentes


            # naopago = docsativos.loc[docsativos['Valor pago'] == 0.0]
            # baixados = naopago.loc[naopago['Ocorrencia'] == 'Baixado co']  
            # naopago = naopago.loc[naopago['Ocorrencia'] != 'Baixado co']  

            # pagos = docsativos.loc[docsativos['Valor pago'] != 0.0]                

            mydb.close()

        return retornosantander_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()


@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def grafeno_view(request):
    codigo_doc = request.GET.get('codigo_doc')
    remessa_data = carregar_dados_remessa_grafeno(codigo_doc)
    retorno_data = carregar_dados_retorno_grafeno(codigo_doc)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc': codigo_doc,

    }

    return render(request, 'grafeno.html', context)



# SANTANDER


def carregar_dados_remessa_santander(codigo_doc=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT 
            `Data da Gravação do Arquivo`,
            `Código de movimento remessa`,   
            `CODIGO DO DOC`,    
            `Data de vencimento do boleto`,	    
            `Valor nominal do boleto`,    
            `Nome do Pagador`,	    
            `Data do desconto 2`,
            `Percentual de Multa`,
            `Data da Multa`,
            `Data Limite para concessão do desconto`,
            `Valor do desconto a ser concedido`,
            `Nº Sequencial do Registro`,
            `Data de emissão do boleto`,
            `Percentual do IOF a ser recolhido`,
            `Número de dias corridos para Protesto`,	
            `Número sequencial do registro no arquivo`,
            `Número sequencial de registro no arquivo trailer`

          FROM remessa_santander            
                """
        if codigo_doc:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessasantander_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc])
            

            remessasantander_bd = remessasantander_bd.loc[remessasantander_bd['CODIGO DO DOC'].notnull() & (remessasantander_bd['CODIGO DO DOC'] != "")]
            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Código de movimento remessa': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasantander_bd.rename(columns=novos_nomes, inplace=True)

            remessasantander_bd = remessasantander_bd.sort_values(by='Data da Ocorrencia', ascending=True)       
        else:
            remessasantander_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)
            remessasantander_bd = remessasantander_bd.loc[remessasantander_bd['CODIGO DO DOC'].notnull() & (remessasantander_bd['CODIGO DO DOC'] != "")]
            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Código de movimento remessa': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasantander_bd.rename(columns=novos_nomes, inplace=True)

            remessasantander_bd = remessasantander_bd.sort_values(by='Data da Ocorrencia', ascending=True)       
            mydb.close()

        return remessasantander_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()


def carregar_dados_retorno_santander(codigo_doc=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT 
            `Data da ocorrência`, 
            `Código movimento retorno`,        
            `CODIGO DO DOC`,
            `Tipo de cobrança`, 
            `Data de vencimento do boleto`,   
            `Valor nominal do boleto`,   
            `Valor total recebido`,       
            `Valor do juros de mora`,   
            `Valor da tarifa cobrada`,
            `Valor de outras despesas`,
            `Valor de juros de atraso`,
            `Valor de IOF recolhido`,
            `Valor do abatimento concedido`,
            `Valor do desconto concedido`,
            `Código de erro 1`,
            `Código de erro 2`,
            `Código de erro 3`,
            `Data da efetivação crédito`,
            `Nome do Pagador`,
            `Número Sequência do arquivo corpo`,
            `Número Sequencial do registro do arquivo corpo`,
            `Código do banco trailer`,
            `Nome do banco`,
            `Número Sequência do arquivo cabecario`,
            `Código do banco`,
            `Número Sequência do arquivo trailer`,
            `Número Sequencial do registro do arquivo trailer`
        FROM retorno_santander
        """
        
        if codigo_doc:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            retornosantander_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc])
            retornosantander_bd['Data da ocorrência'] = pd.to_datetime(retornosantander_bd['Data da ocorrência'])
            retornosantander_bd = retornosantander_bd.sort_values(by='Data da ocorrência')        

            novos_nomes = {
                'Data da ocorrência': 'Data da Ocorrencia',

                'Código movimento retorno': 'Ocorrencia',
            }
            retornosantander_bd.rename(columns=novos_nomes, inplace=True)            
        else:
            retornosantander_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)

            retornosantander_bd['Data da ocorrência'] = pd.to_datetime(retornosantander_bd['Data da ocorrência'])
            retornosantander_bd = retornosantander_bd.sort_values(by='Data da ocorrência')

            novos_nomes = {
                'Data da ocorrência': 'Data da Ocorrencia',
                'Código movimento retorno': 'Ocorrencia',
            }
            retornosantander_bd.rename(columns=novos_nomes, inplace=True)

        return retornosantander_bd

    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()


@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def santander_view(request):
    codigo_doc = request.GET.get('codigo_doc')
    remessa_data = carregar_dados_remessa_santander(codigo_doc)
    retorno_data = carregar_dados_retorno_santander(codigo_doc)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc': codigo_doc,

    }

    return render(request, 'santander.html', context)


# SOFISA


def carregar_dados_remessa_sofisa(codigo_doc_sofisa=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT 
            `Data da Gravação do Arquivo`,
            `Código de movimento remessa`,   
            `CODIGO DO DOC`,     
            `Data de vencimento do boleto`,    
            `Valor nominal do boleto`,    
            `Nome do Pagador`,    
            `Data Limite para concessão do desconto`,
            `Valor do desconto a ser concedido`,
            `Nº Sequencial do Registro`,
            `Primeira instrução`,
            `Segunda instrução`,
            `Data de emissão do boleto`,
            `Valor de Mora dia`,
            `Percentual do IOF a ser recolhido`,
            `Valor do abatimento ou Valor do segundo desconto`,
            `Número de dias corridos para Protesto`,
            `Número sequencial do registro no arquivo`,
            `Número sequencial de registro no arquivo trailer`
        FROM remessa_sofisa       
          """
        if codigo_doc_sofisa:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessasofisa_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_sofisa])
            

            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Código de movimento remessa': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasofisa_bd.rename(columns=novos_nomes, inplace=True)
            remessasofisa_bd = remessasofisa_bd.sort_values(by='Data da Ocorrencia', ascending=True)

        else:
            remessasofisa_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)
            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Código de movimento remessa': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasofisa_bd.rename(columns=novos_nomes, inplace=True)
            remessasofisa_bd = remessasofisa_bd.sort_values(by='Data da Ocorrencia', ascending=True)
        return remessasofisa_bd

    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()
    

    
def carregar_dados_retorno_sofisa(codigo_doc_sofisa=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT 
            `Data da ocorrência`,
            `Código movimento retorno`,        
            `CODIGO DO DOC`,
            `Tipo de cobrança`, 
            `Data de vencimento do boleto`,   
            `Valor nominal do boleto`,   
            `Valor total recebido`,       
            `Valor do juros de mora`,   
            `Valor da tarifa cobrada`,
            `Valor de outras despesas`,
            `Valor de juros de atraso`,
            `Valor de IOF recolhido`,
            `Valor do abatimento concedido`,
            `Valor do desconto concedido`,
            `Código de erro 1`,
            `Código de erro 2`,
            `Código de erro 3`,
            `Data de vencimento do boleto`,
            `Valor de outros créditos`,
            `Valor do IOF em outra unidade`,
            `Número Sequencial do registro do arquivo corpo`,
            `Número Sequencial do registro no arquivo cabeacrio`,
            `Número Sequência do arquivo trailer`,
            `Número Sequência do arquivo corpo`
        FROM retorno_sofisa
             """
        if codigo_doc_sofisa:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            retornosofisa_db = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_sofisa])
            retornosofisa_db['Data da ocorrência'] = pd.to_datetime(retornosofisa_db['Data da ocorrência'])

            novos_nomes = {
                'Data da ocorrência': 'Data da Ocorrencia',
                'Código movimento retorno': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }



            retornosofisa_db.rename(columns=novos_nomes, inplace=True)

            retornosofisa_db = retornosofisa_db.sort_values(by=['Data da Ocorrencia', 'Número Sequência do arquivo corpo'], ascending=[True, True])


            retornosofisa_db.loc[retornosofisa_db['Ocorrencia'] == '28', 'Ocorrencia'] = "Débito Tarifas/Custas Correspondentes"

            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '4', 'Tipo de cobrança'] = "Título Descontado"
            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '2', 'Tipo de cobrança'] = "Cobrança Vinculada"
            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '3', 'Tipo de cobrança'] = "Cobrança Caucionada"
        else:    
            retornosofisa_db = pd.read_sql(consulta_remessa, con=mydb).head(10)


            retornosofisa_db['Data da ocorrência'] = pd.to_datetime(retornosofisa_db['Data da ocorrência'])

            novos_nomes = {
                'Data da ocorrência': 'Data da Ocorrencia',
                'Código movimento retorno': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }



            retornosofisa_db.rename(columns=novos_nomes, inplace=True)

            retornosofisa_db = retornosofisa_db.sort_values(by=['Data da Ocorrencia', 'Número Sequência do arquivo corpo'], ascending=[True, True])


            retornosofisa_db.loc[retornosofisa_db['Ocorrencia'] == '28', 'Ocorrencia'] = "Débito Tarifas/Custas Correspondentes"

            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '4', 'Tipo de cobrança'] = "Título Descontado"
            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '2', 'Tipo de cobrança'] = "Cobrança Vinculada"
            retornosofisa_db.loc[retornosofisa_db['Tipo de cobrança'] == '3', 'Tipo de cobrança'] = "Cobrança Caucionada"


        return retornosofisa_db
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()                        

@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def sofisa_view(request):
    codigo_doc_sofisa = request.GET.get('codigo_doc_sofisa')
    remessa_data = carregar_dados_remessa_sofisa(codigo_doc_sofisa)
    retorno_data = carregar_dados_retorno_sofisa(codigo_doc_sofisa)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc_sofisa': codigo_doc_sofisa,

    }

    return render(request, 'sofisa.html', context)


# SICOOB


def carregar_dados_remessa_sicoob(codigo_doc_sicoob=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
            SELECT 
                `Data de Geração do Arquivo header`,
                `Ocorrencia`,
                `CODIGO DO DOC`,
                `Valor do Título`
            FROM remessa_sicoob
            """
        
        if codigo_doc_sicoob:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_sicoob])        
            remessasafra_bd['Data de Geração do Arquivo header'] = pd.to_datetime(remessasafra_bd['Data de Geração do Arquivo header'], format='%d%m%Y')


            remessasafra_bd = remessasafra_bd.sort_values(by=['Data de Geração do Arquivo header'], ascending=[True])

            novos_nomes = {
                'Data de Geração do Arquivo header': 'Data da Ocorrencia',
                'Ocorrencia': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasafra_bd.rename(columns=novos_nomes, inplace=True)
        else:
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)
            remessasafra_bd['Data de Geração do Arquivo header'] = pd.to_datetime(remessasafra_bd['Data de Geração do Arquivo header'], format='%d%m%Y')
            remessasafra_bd = remessasafra_bd.sort_values(by=['Data de Geração do Arquivo header'], ascending=[True])
            novos_nomes = {
                'Data de Geração do Arquivo header': 'Data da Ocorrencia',
                'Ocorrencia': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            mydb.close()

        return remessasafra_bd
    

    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()
    


def carregar_dados_retorno_sicoob(codigo_doc_sicoob=None):
    try:
        # Conexão com o banco de dados        
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_retorno = """
            SELECT 
                `Data de Geração do Arquivo header`,
                `Ocorrencia`,
                `CODIGO DO DOC`,
                `Carteira`,
                `Valor do Título`,
                `Vencimento`,
                `Nº do Registro`
            FROM retorno_sicoob
        """
        if codigo_doc_sicoob:
            consulta_retorno += " WHERE `CODIGO DO DOC` = %s"
            safraretorno_db = pd.read_sql(consulta_retorno, con=mydb, params=[codigo_doc_sicoob])
            mydb.close()

            new_order = [
            'Data de Geração do Arquivo header',   
            'Ocorrencia',  
            'CODIGO DO DOC',
            'Carteira', 
            'Valor do Título',
            'Vencimento',
            'Nº do Registro',

            ]

            safraretorno_db = safraretorno_db[new_order]
            safraretorno_db['Data de Geração do Arquivo header'] = pd.to_datetime(safraretorno_db['Data de Geração do Arquivo header'], format='%d%m%Y')

            # safraretorno_db['Data da Ocorrencia']  = pd.to_datetime(safraretorno_db['Data da Ocorrencia'])
            safraretorno_db['Vencimento'] = pd.to_datetime(safraretorno_db['Vencimento'], format='%d%m%Y')


            safraretorno_db = safraretorno_db.sort_values(by=['Data de Geração do Arquivo header', 'Nº do Registro'], ascending=[True, True])
        else:
            safraretorno_db = pd.read_sql(consulta_retorno, con=mydb).head(10)
            new_order = [
            'Data de Geração do Arquivo header',   
            'Ocorrencia',  
            'CODIGO DO DOC',
            'Carteira', 
            'Valor do Título',
            'Vencimento',
            'Nº do Registro',

            ]

            safraretorno_db = safraretorno_db[new_order]
            safraretorno_db['Data de Geração do Arquivo header'] = pd.to_datetime(safraretorno_db['Data de Geração do Arquivo header'], format='%d%m%Y')

            # safraretorno_db['Data da Ocorrencia']  = pd.to_datetime(safraretorno_db['Data da Ocorrencia'])
            safraretorno_db['Vencimento'] = pd.to_datetime(safraretorno_db['Vencimento'], format='%d%m%Y')


            safraretorno_db = safraretorno_db.sort_values(by=['Data de Geração do Arquivo header', 'Nº do Registro'], ascending=[True, True])
        
        return safraretorno_db
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()
    

@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def sicoob_view(request):
    codigo_doc_sicoob = request.GET.get('codigo_doc_sicoob')
    remessa_data = carregar_dados_remessa_sicoob(codigo_doc_sicoob)
    retorno_data = carregar_dados_retorno_sicoob(codigo_doc_sicoob)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc_sicoob': codigo_doc_sicoob,

    }

    return render(request, 'sicoob.html', context)


# SAFRA
def carregar_dados_remessa_safra(codigo_doc_safra=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT
            `Data da Gravação do Arquivo`,
                `Cod. Ocorrência`,
                `CODIGO DO DOC`,
                `Valor Do Título`, 
                `Vencimento`,
                `Cod. Carteira`,
                `Nome Do Pagador`, 
                `Data De Emissão Do Título`,
                `Valor Do Desconto Concedido`,
                `dias protesto`, 
                `Primeira Instrução De Cobrança`,
                `Segunda Instrução De Cobrança`, 
                `Juros 1 Dia`,
                `Data Limite Para Desconto`, 
                `Valor lof`, 
                `Número Sequenc. De Registro De Arquivo`,
                `Nº Sequencial do Arquivo`, 
                `Nº Sequencial do Registro`,
                `Número sequencial de registro no arquivo trailer`
                FROM remessa_safra
                        """
        # Reordena as colunas do DataFrame
        if codigo_doc_safra:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_safra])
            
            remessasafra_bd = remessasafra_bd.sort_values(by='Data da Gravação do Arquivo', ascending=True)

            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Cod. Ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasafra_bd.rename(columns=novos_nomes, inplace=True)

            remessasafra_bd['Data da Ocorrencia'] = pd.to_datetime(remessasafra_bd['Data da Ocorrencia'])

            # Ordenar o DataFrame pela coluna 'Data da Ocorrência' de forma crescente
            #df_retorno = df_retorno.sort_values(by='DATA DA GERAÇÃO DO ARQUIVO')
            remessasafra_bd = remessasafra_bd.sort_values(by=['Data da Ocorrencia', 'Número Sequenc. De Registro De Arquivo'], ascending=[True, True])
        else:
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)
            
            remessasafra_bd = remessasafra_bd.sort_values(by='Data da Gravação do Arquivo', ascending=True)

            novos_nomes = {
                'Data da Gravação do Arquivo': 'Data da Ocorrencia',
                'Cod. Ocorrência': 'Ocorrencia',
                # ... adicione os outros nomes conforme necessário
            }
            remessasafra_bd.rename(columns=novos_nomes, inplace=True)

            remessasafra_bd['Data da Ocorrencia'] = pd.to_datetime(remessasafra_bd['Data da Ocorrencia'])

            # Ordenar o DataFrame pela coluna 'Data da Ocorrência' de forma crescente
            #df_retorno = df_retorno.sort_values(by='DATA DA GERAÇÃO DO ARQUIVO')
            remessasafra_bd = remessasafra_bd.sort_values(by=['Data da Ocorrencia', 'Número Sequenc. De Registro De Arquivo'], ascending=[True, True])
                    

        return remessasafra_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()                    
    

def carregar_dados_retorno_safra(codigo_doc_safra=None):
    try:
        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )
        consulta_remessa = """
        SELECT
            `Data Da Ocorrência No Banco`,   
            `Identifica  o Da Ocorrência (Retorno)`,  
            `CODIGO DO DOC`,
            `Identificação Do Tipo De Carteira`, 
            `Valor Título`,
            `Valor Líquido Pago Pelo Pagador`,
            `Data De Vencimento Do Título`,
            `Valor Do Desconto Concedido`, 
            `Tarifa De Cobrança`,
            `indica entrada do titulo no DDA`,
            `Identificaçao Do Registro Transação`, 
            `Tipo De Inscri ão Da Empresara`, 
            `Número De Inscriçao Da Empresa`,
            `Identifica  o Do Título No Banco`,
            `Cod. Ocorrência Recebida No Arquivo REMESSA`,
            `Código De Motivo De Rejeição`, 
            `Data Da Geração Do Arquivo Retorno`,
            `Número Seqüencial Geração Arquivo Retorno treiler`,
            `Número Seqüencial Do Registro No Arquivo treiler`

          FROM retorno_safra            
                    """
        if codigo_doc_safra:
            consulta_remessa += " WHERE `CODIGO DO DOC` = %s"
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb, params=[codigo_doc_safra])
            
            safraretorno_db['Data da Ocorrencia']  = pd.to_datetime(safraretorno_db['Data da Ocorrencia'])
            #df_retorno['Data De Vencimento Do Título']  = pd.to_datetime(df_retorno['Data De Vencimento Do Título'])


            safraretorno_db = safraretorno_db.sort_values(by=['Data da Ocorrencia', 'Número Seqüencial Do Registro No Arquivo'], ascending=[True, True])
                

            safraretorno_db.loc[safraretorno_db['Ocorrencia'] == 'REMESSA DE TÍTULOS', 'Ocorrencia'] = "ENVIADO"
            safraretorno_db['Data De Vencimento Do Título'] = pd.to_datetime(safraretorno_db['Data De Vencimento Do Título'], format='%d%m%y', errors='coerce')

            safraretorno_db['Data De Vencimento Do Título'] = safraretorno_db['Data De Vencimento Do Título'].dt.strftime('%d/%m/%Y')

        else:
            remessasafra_bd = pd.read_sql(consulta_remessa, con=mydb).head(10)
            safraretorno_db['Data da Ocorrencia']  = pd.to_datetime(safraretorno_db['Data da Ocorrencia'])
            #df_retorno['Data De Vencimento Do Título']  = pd.to_datetime(df_retorno['Data De Vencimento Do Título'])


            safraretorno_db = safraretorno_db.sort_values(by=['Data da Ocorrencia', 'Número Seqüencial Do Registro No Arquivo'], ascending=[True, True])
                

            safraretorno_db.loc[safraretorno_db['Ocorrencia'] == 'REMESSA DE TÍTULOS', 'Ocorrencia'] = "ENVIADO"
            safraretorno_db['Data De Vencimento Do Título'] = pd.to_datetime(safraretorno_db['Data De Vencimento Do Título'], format='%d%m%y', errors='coerce')

            safraretorno_db['Data De Vencimento Do Título'] = safraretorno_db['Data De Vencimento Do Título'].dt.strftime('%d/%m/%Y')

        return remessasafra_bd
    except mysql.connector.Error as e:
        print("Erro ao conectar-se ao banco de dados:", e)
        return pd.DataFrame()
    
                        
@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticad
def safra_view(request):
    codigo_doc_safra = request.GET.get('codigo_doc_safra')
    remessa_data = carregar_dados_remessa_sofisa(codigo_doc_safra)
    retorno_data = carregar_dados_retorno_sofisa(codigo_doc_safra)

    context = {
        'remessa': remessa_data.to_dict(orient='records'),
        'retorno': retorno_data.to_dict(orient='records'),
        'codigo_doc_safra': codigo_doc_safra,

    }

    return render(request, 'safra.html', context)


