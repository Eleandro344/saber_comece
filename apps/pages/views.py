from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
# Create your views here.
@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticado
def home_view(request):
    return render(request, 'index.html')

# apps/views.py

# apps/views.py

# apps/views.py

from django.shortcuts import render
import mysql.connector

def unicred_view(request):
    resultados_remessa = []
    resultados_retorno = []

    if request.method == 'POST':
        codigo_doc = request.POST.get('codigo_doc')

        # Conexão com o banco de dados
        mydb = mysql.connector.connect(
            host='db_sabertrimed.mysql.dbaas.com.br',
            user='db_sabertrimed',
            password='s@BRtR1m3d',  
            database='db_sabertrimed',
        )

        cursor = mydb.cursor(dictionary=True)

        # Primeira consulta: Tabela unicredremessa
        consulta_remessa = """
        SELECT 
            `Data da Gravação do Arquivo` AS data_gravacao,    
            `Identificação da Ocorrência` AS identificacao_ocorrencia,
            `CODIGO DO DOC` AS codigo_doc,    
            `Data de emissão do Título` AS data_emissao,    
            `Data de vencimento do Título` AS data_vencimento,    
            `Valor do Título` AS valor_titulo,    
            `Nome/Razão Social do Pagador` AS nome_pagador,  
            `Data Limite P/Concessão de Desconto` AS data_limite_desconto,
            `Valor do Desconto` AS valor_desconto,
            `Nº Sequencial do Registro remessa` AS sequencial_registro_remessa, 
            `Nº Sequencial do Arquivo` AS sequencial_arquivo,
            `Nº Sequencial do Registro` AS sequencial_registro
        FROM unicredremessa
        WHERE `CODIGO DO DOC` = %s
        """
        cursor.execute(consulta_remessa, (codigo_doc,))
        resultados_remessa = cursor.fetchall()

        # Segunda consulta: Tabela retornounicred
        consulta_retorno = """
        SELECT 
            `DATA DA GERAÇÃO DO ARQUIVO` AS data_geracao,   
            `TIPO DE INSTRUÇÃO ORIGEM` AS tipo_instrucao,
            `CÓDIGO DE MOVIMENTO` AS codigo_movimento,
            `CODIGO DO DOC` AS codigo_doc,
            `COMPLEMENTO DO MOVIMENTO` AS complemento_movimento,
            `DATA LIQUIDAÇÃO` AS data_liquidacao,
            `CANAL DE LIQUIDAÇÃO` AS canal_liquidacao,
            `VALOR DO TÍTULO` AS valor_titulo,
            `VALOR ABATIMENTO` AS valor_abatimento,
            `VALOR PAGO` AS valor_pago,
            `JUROS DE MORA` AS juros_mora,
            `VALOR LÍQUIDO` AS valor_liquido,
            `DATA DE VENCIMENTO` AS data_vencimento,
            `DATA PROGRAMADA PARA REPASSE` AS data_repasse,
            `VALOR DA TARIFA` AS valor_tarifa,
            `DATA DE DEBITO DA TARIFA` AS data_debito_tarifa,
            `VALOR DESCONTO CONCEDIDO` AS valor_desconto_concedido,
            `SEQUENCIAL DO REGISTRO` AS sequencial_registro,
            `NOME DO BENEFICIÁRIO` AS nome_beneficiario,
            `SEQUENCIAL DO RETORNO` AS sequencial_retorno
        FROM retornounicred
        WHERE `CODIGO DO DOC` = %s
        """
        cursor.execute(consulta_retorno, (codigo_doc,))
        resultados_retorno = cursor.fetchall()
        

        cursor.close()
        mydb.close()

    # Passando os resultados das duas tabelas para o template
    return render(request, 'unicred.html', {
        'resultados_remessa': resultados_remessa,
        'resultados_retorno': resultados_retorno,
    })
