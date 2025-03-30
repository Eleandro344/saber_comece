from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import mysql.connector
import pandas as pd
import base64
import json
from requests_pkcs12 import post
import pycurl
from django.shortcuts import render, redirect
from .forms import CertificadoForm
from django.contrib import messages
import os
from io import BytesIO
import time
import re 
import mysql.connector
from datetime import datetime, timedelta
from django.shortcuts import render
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os




# Create your views here.
@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticado
def home_view(request):
    return render(request, 'index.html')






@login_required(login_url='login')  # Redireciona para a página de login se o usuário não estiver autenticado
def integracontador(request):
    return render(request, 'integracontador.html')









# Definição das variáveis globais
caminho_certificado = None
senha = None
salvar = None

def trocar_certificado(request):
    global caminho_certificado, senha, salvar  # Declarar como global

    if request.method == 'POST':
        form = CertificadoForm(request.POST)
        if form.is_valid():
            caminho_certificado = form.cleaned_data['certificado']
            senha = form.cleaned_data['senha']
            salvar = form.cleaned_data['salvar']

            if os.path.exists(caminho_certificado):
                request.session['certificado'] = caminho_certificado  
                request.session['senha_certificado'] = senha
                request.session['salvar'] = salvar
                messages.success(request, "Certificado atualizado com sucesso.")

                caminho_certificado = caminho_certificado.replace('\\', '/')
                salvar = salvar.replace('\\', '/')

                print(caminho_certificado)
                print(senha)
                print(salvar)

                # return redirect('pagina_sucesso')  # Redirecionar para uma página de sucesso
            else:
                messages.error(request, "Certificado não encontrado. Verifique o caminho.")
    else:
        form = CertificadoForm()

    return render(request, 'trocar_certificado.html', {'form': form})

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib import messages
# Agora você pode acessar as variáveis fora da função
print(caminho_certificado)
print(senha)
print(salvar)

def serpro_componentes_autenticacaoLoja():
    url = "https://autenticacao.sapi.serpro.gov.br/authenticate"
    if not caminho_certificado or not senha or not salvar:
        print("Erro:Certificado, senha e salvar devem ser preenchidos.")
    else:    
        print("Relatórios sendo gerados...")  # Substituir pelo código real


        # credenciais loja Serpro para autenticação
        consumer_key = "QQzNZnYfhaMRRxJELAtHEd6CNXwa"
        consumer_secret = "8DfDDQYme4MfWpKYy1E4EgmSzkMa"

        # converte as credenciais para base64
        def converter_base64(credenciais):
            return base64.b64encode(credenciais.encode("utf8")).decode("utf8")

        # autenticar na loja com o certificado digital do contratante
        def autenticar(ck, cs, certificado, senha):
            headers = {
                "Authorization": "Basic " + converter_base64(ck + ":" + cs),
                "role-type": "TERCEIROS",
                "content-type": "application/x-www-form-urlencoded"
            }
            body = {'grant_type': 'client_credentials'}
            return post(url,
                        data=body,
                        headers=headers,
                        verify=True,
                        pkcs12_filename=certificado,
                        pkcs12_password=senha)

        return autenticar(consumer_key, consumer_secret, caminho_certificado, senha)


    response = serpro_componentes_autenticacaoLoja()

    print(response.status_code)
    #print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4, separators=(',', ': '), sort_keys=True))
    resposta = json.loads(response.content.decode("utf-8"))
    token=resposta['access_token']    
    jwt_token=resposta['jwt_token']    
    print(token)
    print(jwt_token)


    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )

    cursor=conn.cursor()
    query ="select *from empresas"
    cursor.execute(query)
    resultados=cursor.fetchall()


    for linha in resultados:
        cnpj=linha[0]
        razao =linha[1]
    # cnpj
        dadospedido=     {
                "contratante": {
                    "numero": "90878448000103", #CNPJ DA NOSSA EMPRESA
                    "tipo": 2
                },
                "autorPedidoDados": {
                    "numero": "90878448000103",#CNPJ DA NOSSA EMPRESA
                    "tipo": 2
                },
                "contribuinte": {
                    "numero": cnpj,#CNPJ DO CLIENTE
                    "tipo": 2
                },
                "pedidoDados": {
                    "idSistema": "SITFIS",
                    "idServico": "SOLICITARPROTOCOLO91",
                    "versaoSistema": "2.0",
                    "dados": ""
                }  
                }

        post_data=json.dumps(dadospedido)
        headers = [
            'jwt_token:' + jwt_token,    
            'Authorization: Bearer ' + token,
            'Content-Type: application/json',
            'Accept: text/pain'
        ]
        buffer = BytesIO()

        c=pycurl.Curl()
        c.setopt(c.URL,'https://gateway.apiserpro.serpro.gov.br/integra-contador/v1/Apoiar')
        c.setopt(c.POSTFIELDS,post_data)
        c.setopt(c.HTTPHEADER,headers)
        c.setopt(c.WRITEDATA,buffer)    
        c.perform()
        status_code= c.getinfo(c.RESPONSE_CODE)
        c.close()

        response = buffer.getvalue()

        print(response)
        resultado = json.loads(response.decode("utf-8"))
        dados=json.loads(resultado['dados'])
        protocolo = dados['protocoloRelatorio']
        print(protocolo)
        espera =dados['tempoEspera']
        time.sleep(espera/1000)    







        #EMITIR O PDF DO RELATORIO FISCAL


        dadospedidoEmitir=     {
                "contratante": {
                    "numero": "90878448000103",
                    "tipo": 2
                },
                "autorPedidoDados": {
                    "numero": "90878448000103",
                    "tipo": 2
                },
                "contribuinte": {
                    "numero": cnpj,
                    "tipo": 2
                },
                "pedidoDados": {
                    "idSistema": "SITFIS",
                    "idServico": "RELATORIOSITFIS92",
                    "versaoSistema": "2.0",
                    "dados": "{ \"protocoloRelatorio\":"+'"'+protocolo+'"'+ "}"
                }  
                }

        post_data_Emitir=json.dumps(dadospedidoEmitir)
        headers = [
            'jwt_token:' + jwt_token,    
            'Authorization: Bearer ' + token,
            'Content-Type: application/json',
            'Accept: text/pain'
        ]
        buffer = BytesIO()

        c=pycurl.Curl()
        c.setopt(c.URL,'https://gateway.apiserpro.serpro.gov.br/integra-contador/v1/Emitir')
        c.setopt(c.POSTFIELDS,post_data_Emitir)
        c.setopt(c.HTTPHEADER,headers)
        c.setopt(c.WRITEDATA,buffer)    
        c.perform()
        c.close()

        responseEmitir = buffer.getvalue()
        resultadoEmitir = json.loads(responseEmitir.decode("utf-8"))
        dadosEmitir=json.loads(resultadoEmitir['dados'])
        pdfbase64=dadosEmitir['pdf']
        with open(f"{salvar}{cnpj}_{razao}.pdf", "wb") as f:
            f.write(base64.b64decode(pdfbase64))
            print(f"Relatorio fiscal cliente {razao} salvo com sucesso!")


def disparar_relatorio(request):
    if request.method == "POST":
        # Aqui você chama a função que processa os relatórios
        serpro_componentes_autenticacaoLoja()

        messages.success(request, "Relatórios disparados com sucesso!")
        return JsonResponse({"message": "Relatórios disparados com sucesso!"})

    return JsonResponse({"error": "Método inválido"}, status=400)


