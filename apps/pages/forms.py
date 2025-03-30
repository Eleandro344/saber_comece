from django import forms

class CertificadoForm(forms.Form):
    certificado = forms.CharField(max_length=255, label="Caminho do Certificado")
    senha = forms.CharField(widget=forms.PasswordInput, label="Senha")
    salvar = forms.CharField(max_length=255, label="Salvar em:")
