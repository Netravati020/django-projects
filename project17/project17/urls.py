"""project17 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path
from app17 import views
from project17 import settings

urlpatterns = [
    path('admin/', admin.site.urls),
    path('',views.showIndex, name='main'),
    path('save_student/',views.saveStudent, name='save_student'),
    path('open_admin/',views.showAdmin, name='admin'),
    path('del_student/<int:student_number>',views.delStudent, name='delete_student'),
    path('update_student/<str:student_name>',views.updateStudent, name='update_student'),
]
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

