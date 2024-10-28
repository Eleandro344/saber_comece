from django.shortcuts import render

from apps.pages import views
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required

def timeout_view(request):
    return render(request, 'timeout.html')





def logout_view(request):
    logout(request)
    return redirect('timeout')
# urls.py


def login_view(request):
    if request.method == 'POST':
        email = request.POST.get('email')
        password = request.POST.get('password')
        user = authenticate(request, email=email, password=password)
        if user is not None:
            login(request, user)
            return redirect('home')
        else:
            messages.error(request, 'Email ou senha inválidos')
    
    # Se o usuário já estiver logado, redireciona para a home
    if request.user.is_authenticated:
        return redirect('home')
    return render(request, 'login.html')

# Home view protegida
@login_required(login_url='login')  # Redireciona para login se o usuário não estiver autenticado
def home_view(request):
    return render(request, 'home.html')



