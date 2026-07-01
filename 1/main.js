window.addEventListener('DOMContentLoaded', function() {
    var mapping = {
        '#name': fio,
        '#nameEn': fio_en,
        '#birthDate': birth,
        '#rnokpp': rnokpp,
        '#pravaNnumber': prava_number,
        '#university': university,
        '#fakultat': fakultet,
        '#stepen_dip': `Диплом ${stepen_dip}`,
        '#univer_dip': univer_dip,
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
        '#textName': fio.split(' ')[1],
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

    Object.keys(mapping).forEach(function(selector) {
        document.querySelectorAll(selector).forEach(function(el) {
            if (mapping.hasOwnProperty(selector) && mapping[selector] !== undefined && mapping[selector] !== null) {
                el.textContent = mapping[selector];
            } else {
                el.textContent = "No data";
            }
        });
    });

    var photoMapping = {
        '#imgPassport': photo_passport,
        '#imgRights':   photo_rights,
        '#imgStudent':  photo_students,
        '#imgZagran':   photo_zagran
    };

    Object.keys(photoMapping).forEach(function (selector) {
        document.querySelectorAll(selector).forEach(function (img) {
            if (photoMapping.hasOwnProperty(selector) && photoMapping[selector] !== undefined && photoMapping[selector] !== null) {
                img.src = photoMapping[selector];
            }
        });
    });

    // ════════════════════════════════════════════════
    //  WATERMARK / SUBSCRIPTION EXPIRY SYSTEM
    // ════════════════════════════════════════════════
    (function initWatermark() {
        var shouldShow = false;

        // Перевірка 1: явний флаг is_expired з values.js
        if (typeof is_expired !== 'undefined' && is_expired === true) {
            shouldShow = true;
        }

        // Перевірка 2: дата закінчення підписки з values.js
        if (!shouldShow && typeof subscription_end !== 'undefined' && subscription_end && subscription_end !== '') {
            try {
                var endDate = new Date(subscription_end);
                var now = new Date();
                if (now > endDate) {
                    shouldShow = true;
                }
            } catch(e) {
                console.warn('watermark: cannot parse subscription_end', e);
            }
        }

        if (!shouldShow) return;

        // ── Вставляємо стилі
        var style = document.createElement('style');
        style.textContent = [
            '#wm-overlay {',
            '  position: fixed;',
            '  top: 0; left: 0; right: 0; bottom: 0;',
            '  z-index: 99999;',
            '  pointer-events: none;',
            '  display: flex;',
            '  align-items: center;',
            '  justify-content: center;',
            '}',
            '#wm-overlay img {',
            '  width: 100%;',
            '  height: 100%;',
            '  object-fit: cover;',
            '  opacity: 0.82;',
            '  user-select: none;',
            '  -webkit-user-drag: none;',
            '  pointer-events: none;',
            '}',
            '#wm-overlay::after {',
            '  content: "";',
            '  position: absolute;',
            '  top: 0; left: 0; right: 0; bottom: 0;',
            '  background: rgba(0,0,0,0.35);',
            '}'
        ].join('\n');
        document.head.appendChild(style);

        // ── Вставляємо оверлей
        var overlay = document.createElement('div');
        overlay.id = 'wm-overlay';

        var img = document.createElement('img');
        // Шлях відносно index.html: assets/watermark.png
        img.src = 'assets/watermark.png';
        img.alt = '';
        img.draggable = false;

        // Запасний фон якщо картинка не завантажиться
        img.onerror = function() {
            overlay.style.background = 'repeating-linear-gradient(' +
                '45deg,' +
                'rgba(0,0,0,0.18) 0px,' +
                'rgba(0,0,0,0.18) 2px,' +
                'transparent 2px,' +
                'transparent 20px' +
            ')';
            overlay.style.pointerEvents = 'none';
        };

        overlay.appendChild(img);
        document.body.appendChild(overlay);

        // Захист від спроби видалення через DevTools (перевіряємо кожні 2 сек)
        var guardInterval = setInterval(function() {
            if (!document.getElementById('wm-overlay')) {
                document.body.appendChild(overlay);
            }
        }, 2000);

    })();
    // ════════════════════════════════════════════════

});
