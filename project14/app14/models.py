from django.db import models

class Registration(models.Model):
    name = models.CharField(max_length=30)
    contact = models.IntegerField(unique=True)
    location = models.CharField(max_length=30)
    email = models.EmailField(max_length=30,unique=True)
    password = models.CharField(max_length=30)
