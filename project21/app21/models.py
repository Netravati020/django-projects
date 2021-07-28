from django.db import models

# Create your models here.

class CustomerModel(models.Model):
    customer_id = models.AutoField(primary_key=True)
    customer_name = models.CharField(max_length=100)
    customer_contact = models.IntegerField()

    def __str__(self):
        return str(self.customer_id)
        #return self.customer_name

class ProductModel(models.Model):
    product_id = models.AutoField(primary_key=True)
    product_name = models.CharField(max_length=100)
    product_price = models.FloatField()

    def __str__(self):
        return self.product_name


class OrderModel(models.Model):
    order_id = models.AutoField(primary_key=True)
    cust_id = models.ForeignKey(CustomerModel,on_delete=models.CASCADE)
    product = models.ManyToManyField(ProductModel)