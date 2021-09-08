from django.db import models

# Create your models here.
class ProductModel(models.Model):
    number = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100,null=False,unique=True)
    price = models.FloatField()
    photo = models.ImageField(upload_to='product_images/')

