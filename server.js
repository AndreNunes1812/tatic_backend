import express  from 'express';
import parquet from 'parquetjs';
import { readwrite } from './readwrite.js';

const PORT = 8080;
const HOST = '0.0.0.0';



readwrite()


/*EXCHANGE_ID    = 8138;
CALLING_NUMBER = 5521987366501;
CALLED_NUMBER  = 5521908100740;
SERIAL_NUMBER  = 5604248321560188;
CALL_TYPE      = 1;
CALL_DURATION  = 98;
START_TIME     = 2019-10-05T05:56:17;
IMSI           = 8136378640573727;
SWITCH         = 39;
CELL_IN        = 65234;
CELL_OUT       = 00056;
TECNOLOGIA     = A;
FILE_NAME      = calls.txt;
FIRST_LAC      = f86a89;
LAST_LAC       = bf1043;
GGSN_ADDRESS   = endpoint.ggsn.tatic.com
*/

// declare a schema for the `CDR` table



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


app.get('/gerar_parquet', async(req, res) => {

  let valor = []
  let ValorJson= []
  let arquivosTodos = []

  const arquivos =   await fs.promises.readdir('./files',function(error,files){});

 // console.log('Arquivos:',arquivos)
  
 // let readFiles = await fs.promises.readFile('./files/' + arquivos[0],'utf8');
 // arquivosTodos.push(readFiles);

 // readFiles = await fs.promises.readFile('./files/' + arquivos[1],'utf8');
 // arquivosTodos.push(readFiles);

 // console.log('Todos:',arquivosTodos)


  // const writeFiles = await fs.promises.writeFile("./output/filesystem.parquet","console.log('Andre Nunes')")

  var writeRow = await parquet.ParquetWriter.openFile(schema, './output/filesystem.parquet');
  
  const file = await fs.readFile('./files/' + arquivos[0], 'utf8', async function (err, data) {

    try {
      
      let zero=0;
      var linhas = data.split(/\r?\n/);
      linhas.forEach(async function (linha) {

        //  console.log('Estou aqui')        
        if (linhas.length >= zero) {
        
           console.log('N:', zero)

           const linhaSplit = linha.split(';')

           valor.push( linhaSplit );
 
           let nameRow= valor[zero][0] 
           let exchange_idRow= valor[zero][0]
           let calling_numberRow = valor[zero][1]
           let called_numberRow = valor[zero][2]

         
           ValorJson.push({
              name: nameRow,
              exchange_id: exchange_idRow ,
              calling_number: calling_numberRow,
              called_number: called_numberRow
           })

           zero++
        
            Promise.all( await writeRow.appendRow( {
              name: nameRow,
              exchange_id: exchange_idRow ,
              calling_number: calling_numberRow,
              called_number: called_numberRow
            } )
            
           );
           await writeRow.close(); 

         

           //console.log('*******************************')
           //console.log(`N:${zero}`, valor[zero][0])
           //console.log(`N:${zero}`, valor[zero][1])
           //console.log(`N:${zero}`, valor[zero][2])
           //console.log('*******************************')
      
       
           
          
//          await writeRow.appendRow({
//            name: nameRow,
//            exchange_id: exchange_idRow ,
 //           calling_number: calling_numberRow,
 //           called_number: called_numberRow
  //        });

    //      await writeRow.close();
          
        }
      });


    } catch (error) {
      console.log('Error:', error);

    } finally {
     
//  console.log('Sai:', JSON.stringify( ValorJson).replace(']','').replace('[',''));
    // await writeRow.appendRow( JSON.stringify( ValorJson).replace(']','').replace('[','') );
     
    }

    console.log('Saiiiiiiiiiiiiiiii');

    // console.log('Valoroooo:', valor[0][0],valor[0][1])
    // console.log('Data:', data.split(';'))
    //   arquivos = data.split(';')
    /*  fs.promises.writeFile('./output/filesystem.txt', data, function(err, result) {
         if(err) console.log('error', err);
       });
       */
  });


 // console.log('File:', JSON.stringify(file))


//AVRO
  
  
 /*const write = await fs.promises.writeFile('./output/filesystem.txt', XPTO, function(err, result) {
    if(err) console.log('error', err);
  });
*/

  // console.log('Write:', write)
  
/*  try {
    await fs.writeFile("/output/filesystem.txt", "console.log('Hello world with Node.js v13 fs.promises!'");
    console.info("File created successfully with Node.js v13 fs.promises!");
  } catch (error){  
    console.error(error);
  }
*/

  res.contentType('application/json');
  return res.send(JSON.stringify(arquivos));
});

app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);