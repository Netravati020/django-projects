from django.shortcuts import render
from django.http import HttpResponse
from app14.models import Registration



def showMainPage(request):
    return render(request,"main.html")

def showRegisterPage(request):

  if request.method == "POST":
        n = request.POST.get("name")
        con = request.POST.get("contact")
        loc = request.POST.get("location")
        email = request.POST.get("email")
        password = request.POST.get("password")

        Registration(name=n,contact=con,location=loc,email=email,password=password).save()

        return render(request,"register.html",{"message":"Registration is Done"})

  else:
        return render(request,"register.html")


def showLoginPage(request):
    if request.method == "GET":
        return render(request,"login.html")
    else:
        em = request.POST.get("email")
        pas = request.POST.get("password")

        try:
            stu_obj = Registration.objects.get(email=em, password=pas)
            return render(request, "welcome.html")

        except Registration.DoesNotExist:
            return render(request,"login.html",{"error_message":"Invalid id User"})



