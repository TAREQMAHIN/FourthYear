// function to get current date and time
function getDateTime() {
    var date = new Date().toJSON().slice(0, 10);
    var time = new Date().toJSON().slice(11, 19)
    return date + ' ' + time;
}

module.exports = {
    timestamp : getDateTime,
}