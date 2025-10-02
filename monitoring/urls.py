from django.urls import path

from . import views

urlpatterns = [
    path('', views.ptc_table, name='ptc_table'),  # Главная страница с таблицей
    path('api/ptc/', views.api_ptc_data, name='api_ptc_data'),  # API для данных
]
