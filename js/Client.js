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
    window.open('https://drive.google.com/file/d/18w0wyAo9za6ngKQHNcDOh6g0Hb4_80Gh/view?usp=drivesdk', '_blank');
});

document.getElementById('goBackForm').addEventListener('submit', function(event) {
    event.preventDefault();
    window.location.href = '/T/index.html';
});
