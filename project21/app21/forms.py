
from django import forms
from app21.models import *


class CustomerForm(forms.ModelForm):
    class Meta:
        model = CustomerModel
        fields = "__all__"


class ProductForm(forms.ModelForm):
    class Meta:
        model = ProductModel
        fields = "__all__"


class OrderForm(forms.ModelForm):
    class Meta:
        model = OrderModel
        fields = "__all__"