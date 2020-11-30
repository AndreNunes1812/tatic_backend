import express  from 'express';
import parquet from 'parquetjs';
import { concatenarArquivos } from './concatenarArquivos.js';

const PORT = 8080;
const HOST = '0.0.0.0';


var schema = new parquet.ParquetSchema({
  name: { type: 'UTF8' },
  exchange_id: { type: 'UINT_64' },
  calling_number: { type: 'UINT_64' },
  called_number:  {  type: 'UINT_64' },
/*  serial_number:  {  type: 'UINT_64' },
  call_type: { type:'UINT_64'},
  call_duration: { type:'UINT_64'},
  start_time: { type: 'TIMESTAMP_MILLIS'},
  imsi: { type: 'UTF8'},
  switch: {type: 'UINT_64'},
  cell_in: { type: 'UTF8'},
  cell_out: {type: 'UTF8'},
  tecnologia: { type: 'UTF8'},
  file_name: { type: 'UTF8'},
  first_lac: {type: 'UTF8'},
  last_lac: { type: 'UTF8'},
  ggsn_address: { type: 'UTF8'},
  date: { type: 'TIMESTAMP_MILLIS' },
  in_stock: { type: 'BOOLEAN' }
  */
});

// App
const app = express();

app.use(express.json())

app.get('/parquet', async(req, res) => {

// reading nested rows with a list of explicit columns
try {
  console.log('1')
   let reader = await parquet.ParquetReader.openFile('./output/filesystem.parquet');
   console.log('2')
   
   let cursor = reader.getCursor([['name'], ['exchange_id'],['calling_number'],['called_number']]);
   let record = null;
  
//   console.log('Cursor:',cursor)
   let total=1
   while (record = await cursor.next()) {
    console.log('Entrei:');

     console.log(record);
     total++
   }
   console.log('Total de Registros:', total)
   await reader.close();
} catch (error) {
  console.log('ERRRRO:', error)  
}
   
  res.contentType('application/json');
  return res.send({message: 'ok'});
  }
)

app.get('/concatenar', async(req, res) => {
 
  concatenarArquivos()

  res.contentType('application/json');
  return res.send({message: 'arquivo sendo concatenado....'});
});

app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);