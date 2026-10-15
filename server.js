const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const cassandra = require('cassandra-driver');

const app = express();
app.use(express.json());
app.use(cors());

app.use(express.static(__dirname));

mongoose.connect('mongodb://admin:admin.123@127.0.0.1:27017/tienda_db?authSource=admin', {
    useNewUrlParser: true,
    useUnifiedTopology: true
})
.then(() => console.log('Conectado a MongoDB (tienda_db)'))
.catch(err => console.error('Error MongoDB:', err));

const productoSchema = new mongoose.Schema({
    sku: { type: String, unique: true, required: true },
    nombre: String,
    categoria: String,
    precio: Number,
    stock: Number,
    atributos: String
});
const Producto = mongoose.model('Producto', productoSchema, 'productos');


const cassandraClient = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1',
    keyspace: 'iot_telemetria'
});

cassandraClient.connect()
    .then(() => console.log('Conectado a Cassandra (iot_telemetria)'))
    .catch(err => console.error('Error Cassandra:', err));


app.get('/mongo/productos', (req, res) => {
    res.sendFile(__dirname + '/productos.html');
});

app.get('/cassandra/telemetria', (req, res) => {
    res.sendFile(__dirname + '/telemetria.html');
});


// ================= APIS MONGODB (CRUD PRODUCTOS) =================
app.get('/api/productos', async (req, res) => {
    try {
        const data = await Producto.find();
        res.json(data);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/productos', async (req, res) => {
    try {
        const nuevo = new Producto(req.body);
        await nuevo.save();
        res.status(201).json({ mensaje: 'Producto creado', nuevo });
    } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/api/productos/:sku', async (req, res) => {
    try {
        const actualizado = await Producto.findOneAndUpdate({ sku: req.params.sku }, req.body, { new: true });
        res.json({ mensaje: 'Producto actualizado', actualizado });
    } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/productos/:sku', async (req, res) => {
    try {
        await Producto.findOneAndDelete({ sku: req.params.sku });
        res.json({ mensaje: 'Producto eliminado' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});


// ================= APIS CASSANDRA IOT =================
app.get('/api/telemetria', async (req, res) => {
    try {
        const result = await cassandraClient.execute('SELECT * FROM lecturas_sensor LIMIT 50');
        res.json(result.rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/telemetria', async (req, res) => {
    const { sensor_id, ubicacion, temperatura, humedad } = req.body;
    const query = 'INSERT INTO lecturas_sensor (sensor_id, fecha, ubicacion, temperatura, humedad) VALUES (?, toTimestamp(now()), ?, ?, ?)';
    const uuidSensor = sensor_id ? cassandra.types.Uuid.fromString(sensor_id) : cassandra.types.Uuid.random();
    
    const params = [uuidSensor, ubicacion, parseFloat(temperatura), parseFloat(humedad)];
    try {
        await cassandraClient.execute(query, params, { prepare: true });
        res.status(201).json({ mensaje: 'Lectura IoT registrada' });
    } catch (e) { res.status(400).json({ error: e.message }); }
});

app.get('/api/telemetria/:sensor_id', async (req, res) => {
    try {
        const query = 'SELECT * FROM lecturas_sensor WHERE sensor_id = ?';
        const result = await cassandraClient.execute(query, [cassandra.types.Uuid.fromString(req.params.sensor_id)], { prepare: true });
        res.json(result.rows);
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

app.get('/api/estadisticas', async (req, res) => {
    try {
        const result = await cassandraClient.execute('SELECT temperatura, humedad FROM lecturas_sensor');
        const rows = result.rows;
        
        if (rows.length === 0) {
            return res.json({ total: 0, tempAvg: 0, humAvg: 0, tempMax: 0, tempMin: 0 });
        }

        let sumaTemp = 0, sumaHum = 0;
        let maxTemp = rows[0].temperatura, minTemp = rows[0].temperatura;

        rows.forEach(r => {
            sumaTemp += r.temperatura;
            sumaHum += r.humedad;
            if (r.temperatura > maxTemp) maxTemp = r.temperatura;
            if (r.temperatura < minTemp) minTemp = r.temperatura;
        });

        res.json({
            total: rows.length,
            tempAvg: (sumaTemp / rows.length).toFixed(2),
            humAvg: (sumaHum / rows.length).toFixed(2),
            tempMax: maxTemp,
            tempMin: minTemp
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});


app.listen(3002, () => {
    console.log('Servidor dual oficial corriendo en puerto 3002');
});