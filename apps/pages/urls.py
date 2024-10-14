from django.urls import path 
from pages import views


urlpatterns = [
    path('home/', views.home_view, name='home'),  # Mapeia a URL 'home/' para a view 'home_view'
    # path('unicred/', views.unicred_view, name='unicred'),
    path('unicred/', views.unicred_view, name='unicred_view'),
    path('faturamento/', views.unicred_view, name='unicred_view'),



]