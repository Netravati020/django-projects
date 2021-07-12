from django.db import models

# Create your models here.
class RegistrationModel(models.Model):
    name = models.CharField(max_length=100,null=False)
    contact = models.IntegerField(unique=True, null=False)
    email = models.EmailField(max_length=100,unique=True, null=False)
    password = models.CharField(max_length=100,null=False)
    otp = models.IntegerField(null=False)
    status = models.CharField(max_length=30, null=False,default="pending")
    datetime = models.DateTimeField(auto_now_add=True)