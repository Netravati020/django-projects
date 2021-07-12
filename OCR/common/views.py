from django.shortcuts import render
from django.shortcuts import redirect
from student.models import RegistrationModel
from common.utils import sendTextMessage
from random import randint
from django.db.models import Q
from django.contrib import messages


def showCommonPage(request):
    return render(request,"common/index.html")

def studentPage(request):
    return render(request,"common/student.html")

def studentRegistration(request):
    if request.method=="POST":
        name= request.POST.get("student_name")
        contact  = request.POST.get("student_contact")
        email = request.POST.get("student_email")
        password = request.POST.get("student_password")

        record= RegistrationModels.object.filter(Q(contact=contact) |Q(email=email))

        if record:
            return render(request,"common/student.html",{"data":[name,contact,email,"Contactnumber or Email is Taken"]})
        else:
            otp = randint(100000, 999999)

            message = '''Thanks for Registration With Sathya,
                      To finish the Registration Use the Given OTP
                      Your OTP : ''' + str(otp)


            if sendTextMessage(message,contact):
                RegistrationModel(name=name,contact=contact,email=email,password=password,otp=otp).save()

                messages.success(request,contact)

                return redirect('student_otp')

            else:
                return render(request,"common/student.html",{"data":[name,contact,email,"Wrong Contact Number"]})
    else:
        studentPage(request)

def openStudentOtp(request):
    return render(request,"common/otp.html")

def studentLoginCheck(request):
    if request.method== "POST":
        email= request.POST.get("student_email")
        password= request.POST.get("student_password")
        return HttpResponse("Under Development")
    else:
        return render(request,"common/student.html")

def validateOtp(request):
    contact= request.POST.get("contact")
    sotp= request.POST.get("student_otp")

    try:
        record= RegistrationModel.objects.get(contact=contact,otp=sotp)
        record.status= 'Active'
        record.save()
        return render(request,"common/student.html",{"message":"Thanks For Registration,Please Login"})

    except RegistrationModel.DoesNotExist:
        messages.success(request, contact)
        return redirect("student_otp")





