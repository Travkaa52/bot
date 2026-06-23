window.addEventListener('DOMContentLoaded', function() {
    var mapping = {
        '#name': fio,
        '#nameEn': fio_en,
        '#birthDate': birth, // или birthDate, если так у тебя называется
        '#rnokpp': rnokpp,
        '#pravaNnumber': prava_number,

        '#university': university,
        '#fakultat': fakultet, // проверь на странице имя id!
        '#stepen_dip': `Диплом ${stepen_dip}`, // Диплом доктора наук
        '#univer_dip': univer_dip, //Університет
        '#dayout_dip': dayout_dip,

        '#special_dip': special_dip,
        '#number_dip': number_dip,
        '#placeBirth': live,
        '#srokPrav': prava_date_out,
        '#adress': bank_adress,

        '#dateGiveZ': dateGiveZ,
        '#dateOutZ': dateOutZ,

        '#sex': sex,
        '#sexEn': sex_en,
        '#textName': fio.split(' ')[1], // только имя, если надо
        '#zagran_number': zagran_number,
        '#nomerStudy': student_number,

        '#vidanoStudy': student_date_give,
        '#diusnuyDoStudy': student_date_out,
        '#formaStudy': form,
        '#rightsCategories': rights_categories,
        '#dateGive': date_give,

        '#dateGivePrava': prava_date_give,
        '#dateOut': date_out,
        '#nomerPasport': pass_number,
        '#organ': organ,
        '#uznr': uznr,
        
        '#legalAdress': legalAdress,
        '#registeredOn': registeredOn,
        '#pravaOrgan': pravaOrgan
    };

   // Используем только один цикл перебора с проверкой
    Object.keys(mapping).forEach(function(selector) {
        document.querySelectorAll(selector).forEach(function(el) {
            // Проверяем наличие ключа в mapping и что значение не undefined и не null
            if (mapping.hasOwnProperty(selector) && mapping[selector] !== undefined && mapping[selector] !== null) {
                el.textContent = mapping[selector];
            } else {
                el.textContent = "No data";
            }
        });
    });

 /**
     * photoMapping — для картинок.
     * Картинки должны иметь id: imgPassport, imgRights, imgStudent, imgZagran.
     * Пути лежат в values.js
    */
    var photoMapping = {
        '#imgPassport': photo_passport,
        '#imgRights':   photo_rights,
        '#imgStudent':  photo_students,
        '#imgZagran':   photo_zagran
    };

    // Меняем src для каждой соответствующей картинки
    Object.keys(photoMapping).forEach(function (selector) {
        document.querySelectorAll(selector).forEach(function (img) {
            if (
                photoMapping.hasOwnProperty(selector) 
                && photoMapping[selector] !== undefined 
                && photoMapping[selector] !== null
            ) {
                img.src = photoMapping[selector];
            }
        });
    });

    // Можно добавить другие init-функции сюда при необходимости.

});
