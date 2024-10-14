from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import mysql.connector
import pandas as pd
# Create your views here.
@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticado
def home_view(request):
    return render(request, 'index.html')

# apps/views.py

# apps/views.py

from django.shortcuts import render
import pandas as pd
import mysql.connector

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



