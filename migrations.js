
var mapFunction1 = function () {

    var removeUndefined = function (obj) {
        Object.keys(obj).forEach(key => {
            if (obj[key] === undefined || obj[key] == null || obj[key] == "") { delete obj[key] }
        });
        return obj;
    }

    var mapHomeAddress = function (obj) {
        if (obj.address != undefined) {
            result = {
                country: obj.address.country,
                province: obj.address.province,
                city: obj.address.city,
                postalCode: obj.address.postalCode,
                streetName: obj.address.streetName,
                streetNumber: obj.address.streetNumber,
                floor: obj.address.floor,
                department: obj.address.department
            }

            return removeUndefined(result)
        }
    }

    var mapWorkAddress = function (obj) {
        if (obj.address != undefined) {
            result = {
                country: obj.address.workCountry,
                province: obj.address.workProvince,
                city: obj.address.workCity,
                postalCode: obj.address.workPostalCode,
                streetName: obj.address.workStreetName,
                streetNumber: obj.address.workStreetNumber,
                floor: obj.address.workFloor,
                department: obj.address.workDepartment
            }
            return removeUndefined(result)
        }
    }


    var mapProduct = function (obj) {

        if (obj.selectedProduct.amount === undefined) { currency = undefined } else { currency = "ARS" }
        amount = { value: obj.selectedProduct.amount, currency: currency }
        result = {
            productId: obj.selectedProduct.productId,
            numberInstallments: obj.selectedProduct.numberInstallments,
            amount: removeUndefined(amount),
            loanInstallment: obj.selectedProduct.loanInstallment,
        }

        return removeUndefined(result)
    }

    var mapContactInfo = function (obj) {
        phone = {
            number: obj.cellPhone,
            area: obj.areaCode,
            validationDate: obj.validatedCellPhone === true ? obj.lastModifiedDate : undefined,
        }

        return removeUndefined(phone)
    }

    var maploanApplication = function (obj) {

        tags = []
        if (obj.isInBlacklist === true) { tags.push("blacklisted") }
        if (obj.isClient === true) { tags.push("is_client") }
        riskInfo = {
            tags: tags,
            scoreId: obj.idScoring,
            dictum: obj.dictamen
        }

        validation = {
            status: obj.nosisOk === false ? "invalid" : "valid"
        }

        account = {
            bankId: obj.bank,
            accountId: obj.cbu
        }

        requestedAmount = { value: obj.requestedAmount, currency: obj.requestedAmount === undefined ? undefined : "ARS" }

        loanApp = {
            created: obj.createdDate,
            finished: obj.lastModifiedDate,
            brand: "PRESTO",
            applicationId: obj.webSocketId,
            step: obj.step,
            status: "finished",

            leadSource: obj.leadSource,
            campaign: obj.campania,
            adgroup: obj.adGroup,
            employmentStatus: obj.employmentStatus,
            requestedAmount: removeUndefined(requestedAmount),
            payDay: obj.payDay,
            selectedProduct: mapProduct(obj),
            riskInfo: removeUndefined(riskInfo),

            identityValidation: removeUndefined(validation),
            account: removeUndefined(account),
            leadAd: obj.leadAd,
            salesforceStatus: obj.salesForceStatus
        }

        return [
            removeUndefined(loanApp)
        ]
    }

    var mapBankData = function (obj) {
        account = {
            bankId: obj.bank,
            accountId: obj.cbu
        }
        return account
    }

    homeAddress = mapHomeAddress(this)
    workAddress = mapWorkAddress(this)
    contactInfo = mapContactInfo(this)
    fingerprintRes = this.fingerPrint === undefined ? undefined : this.fingerPrint.values
    account = mapBankData(this)
    documents = this.documents
    loanApplication = maploanApplication(this)

    object_result = {
        created: this.createdDate,
        updated: this.lastModifiedDate,

        fiscalId: this.fiscalId,
        nationalId: this.documents != undefined ? this.documents.dni : undefined,
        gender: this.gender,
        birthdate: this.birthDate,
        lastName: this.lastName,
        firstName: this.firstName,
        email: this.email != null ? this.email : "",
        homeAddress: homeAddress,
        workAddress: workAddress,
        documents: documents,
        phones: [contactInfo],
        loanApplications: loanApplication,
        accounts: [account],
        fingerprint: fingerprintRes,
        _class: "com.wenance.service.customer.models.Customer"
    }

    emit(this._id, removeUndefined(object_result));
}

var reduceFunction1 = function (key, values) {
    return values;
}

var db = db.getSiblingDB('dev')

db.customers.renameCollection("customers_ori")

db.customers_ori.mapReduce(
    mapFunction1,
    reduceFunction1,
    {
        out: "customers",
    }
)

db.customers.find().forEach(function (item) {
    db.customers.update({ _id: item._id }, item.value);
});

