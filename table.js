
// Structure to save Table Information
class Table {
    constructor(name, property, primary_key) {
        this.name = name;
        this.property = property;
        this.primary_key = primary_key;
    }

    print() {
        console.log('Name : ' + this.name);
        console.log('Properties : ' + this.property);
        console.log('PrimaryKey : ' + this.primary_key);
    }
}

module.exports = {
    Table : Table,
}