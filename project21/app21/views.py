from django.shortcuts import render,redirect

# Create your views here.
from app21.forms import *
from app21.models import *


def showIndex(request):
    cf = CustomerForm(request.POST)
    if request.method == "POST":
        cf.save()
        return redirect('main')
    else:
        return render(request,"index.html",{"cform":cf,"cdata":CustomerModel.objects.all()})


def showProduct(request):
    pf = ProductForm(request.POST)
    if request.method == "POST":
        pf.save()
        return redirect('product')
    else:
        return render(request, "product.html", {"pform": pf, "pdata": ProductModel.objects.all()})


def showOrder(request):
    pf = OrderForm(request.POST)
    if request.method == "POST":
        pf.save()
        return redirect('order')
    else:
        return render(request, "order.html", {"oform": pf, "odata": OrderModel.objects.all()})