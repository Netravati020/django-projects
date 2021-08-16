from django.shortcuts import render

# Create your views here.
from django.shortcuts import render,redirect
from app22.models import ProductModel

def openMainPage(request):
    if request.method == "POST":
        name = request.POST.get("product_name")
        price = request.POST.get("product_price")
        photo = request.FILES['product_photo']
        ProductModel(name=name,price=price,photo=photo).save()
        return redirect('main')

    else:
        return render(request,"index.html")

def viewall(request):
    products_list = ProductModel.objects.all()

    total_cookies= len(request.COOKIES)
    return render(request, "viewall.html", {"all_products": products_list,"cookie": total_cookies})

def save_cookie(request):
    product_number= request.GET.get('pno')
    product_name= request.GET.get('pname')

    response = redirect('view_all')
    response.set_cookie(product_number, product_name)
    return response



