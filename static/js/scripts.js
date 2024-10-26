// scripts.js

document.addEventListener('DOMContentLoaded', function () {
    // Smooth scroll effect for links (like the Next button on the welcome page)
    const smoothScrollLinks = document.querySelectorAll('a[href^="#"]');
    smoothScrollLinks.forEach(function (link) {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            target.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        });
    });

    // Validate form fields (e.g., flight ID, carrier name)
    const forms = document.querySelectorAll('form');
    forms.forEach(function (form) {
        form.addEventListener('submit', function (event) {
            let isValid = true;

            // Check all required inputs
            const inputs = form.querySelectorAll('input[required]');
            inputs.forEach(function (input) {
                if (!input.value.trim()) {
                    input.classList.add('error');
                    isValid = false;
                } else {
                    input.classList.remove('error');
                }
            });

            if (!isValid) {
                event.preventDefault();
                alert('Please fill out all required fields.');
            }
        });
    });

    // Clear error styles when input changes
    const inputFields = document.querySelectorAll('input');
    inputFields.forEach(function (input) {
        input.addEventListener('input', function () {
            if (input.value.trim()) {
                input.classList.remove('error');
            }
        });
    });
});
