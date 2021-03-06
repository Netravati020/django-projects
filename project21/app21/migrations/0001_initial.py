# Generated by Django 3.2.3 on 2021-07-28 06:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CustomerModel',
            fields=[
                ('customer_id', models.AutoField(primary_key=True, serialize=False)),
                ('customer_name', models.CharField(max_length=100)),
                ('customer_contact', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='ProductModel',
            fields=[
                ('product_id', models.AutoField(primary_key=True, serialize=False)),
                ('product_name', models.CharField(max_length=100)),
                ('product_price', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='OrderModel',
            fields=[
                ('order_id', models.AutoField(primary_key=True, serialize=False)),
                ('cust_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='app21.customermodel')),
                ('product', models.ManyToManyField(to='app21.ProductModel')),
            ],
        ),
    ]
