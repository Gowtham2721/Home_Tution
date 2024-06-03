// Client.js

document.getElementById('detailsForm1').addEventListener('submit', function(event) {
    event.preventDefault();
    window.open('https://docs.google.com/spreadsheets/d/1zOIaV8xbkv81Cf_1cMWokTuFJYryW94HoNu2zr-XMoo/edit?usp=sharing', '_blank');
    // window.location.href = 'teju.html';
});

document.getElementById('detailsForm2').addEventListener('submit', function(event) {
    event.preventDefault();
    window.open('https://docs.google.com/spreadsheets/d/1gGJeLZ1EZPg1-s94NRYm3pfKVg_Y64bypBHXdpsSBbA/edit?usp=sharing', '_blank');
    // window.location.href = 't.html';
});

document.getElementById('amountForm').addEventListener('submit', function(event) {
    event.preventDefault();
    window.open('https://www.justdial.com/jdmart/Vijayawada/Techno-Enterprises-Ring-Road-Ring-Road/0866PX866-X866-111022164442-M5G3_BZDET/catalogue?ref=auto&searchfrom=auto|home&trkid=6103325764', '_blank');
});

document.getElementById('goBackForm').addEventListener('submit', function(event) {
    event.preventDefault();
    window.location.href = '/T/index.html';
});
