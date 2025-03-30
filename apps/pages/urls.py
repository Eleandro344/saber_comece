from django.urls import path 
from pages import views
from .views import disparar_relatorio

urlpatterns = [
    path('home/', views.home_view, name='home'),  # Mapeia a URL 'home/' para a view 'home_view'
    path('trocar_certificado/', views.trocar_certificado, name='trocar_certificado'),  # Mapeia a URL 'home/' para a view 'home_view'
    # path('sittax/', views.sittax_view, name='sittax_view'),
    path('disparar-relatorio/', disparar_relatorio, name='disparar_relatorio'),


    

]