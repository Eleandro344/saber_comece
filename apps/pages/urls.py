from django.urls import path 
from pages import views


urlpatterns = [
    path('home/', views.home_view, name='home'),  # Mapeia a URL 'home/' para a view 'home_view'
    # path('unicred/', views.unicred_view, name='unicred'),
    path('unicred/', views.unicred_view, name='unicred_view'),
    path('faturamento/', views.faturamento_view, name='faturamento_view'),
    path('docsitau/', views.itau_view, name='itau_view'),
    path('grafeno/', views.grafeno_view, name='grafeno_view'),





]