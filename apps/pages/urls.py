from django.urls import path 
from pages import views


urlpatterns = [
    path('home/', views.home_view, name='home'),  # Mapeia a URL 'home/' para a view 'home_view'
    # path('unicred/', views.unicred_view, name='unicred'),
    path('unicred/', views.unicred_view, name='unicred_view'),
    path('faturamento/', views.faturamento_view, name='faturamento_view'),
    path('docsitau/', views.itau_view, name='itau_view'),
    path('grafeno/', views.grafeno_view, name='grafeno_view'),
    path('santander/', views.santander_view, name='santander_view'),
    path('sofisa/', views.sofisa_view, name='sofisa_view'),
    path('sicoob/', views.sicoob_view, name='sicoob_view'),
    path('safra/', views.safra_view, name='safra_view'),
    

]