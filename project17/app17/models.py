from django.db import models

class StudentDetailsModel(models.Model):
    number=  models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    contact = models.IntegerField(unique=True)
    photo = models.ImageField(upload_to='student_images')
    dob = models.DateField()
    email = models.EmailField(max_length=100)
    password = models.CharField(max_length=50)
    date_of_reg= models.DateField(auto_now_add=True)










