function (doc) {
    if (doc.timestamp) {
        var date = new Date(doc.timestamp);
        var total = 0.0;
        for (currency in doc) {
            if (typeof(doc[currency]) != "string" ) {
                for (exchange in doc[currency]) {
                    total += doc[currency][exchange]['amount'] * doc[currency][exchange]['bid'];
                }
            }
        }
        emit([date.getFullYear(), date.getMonth() + 1, date.getDate(), doc.adjusted], total);
    }
}