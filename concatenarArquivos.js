import parquet from 'parquetjs';
import { dirname, join } from 'path' 
import { promisify } from 'util'
import { promises, createReadStream, createWriteStream } from 'fs'
import { pipeline, Transform } from 'stream'
const pipelineAsync = promisify(pipeline)

import csvtojson from 'csvtojson'

import StreamConcat from 'stream-concat'

const { readdir } = promises
import debug from 'debug'
import { STATUS_CODES } from 'http'
const log = debug('app:concat')


const { pathname: currentFile } = new URL(import.meta.url)
const cwd = dirname(currentFile)
const filesDir = `${cwd}/files`
const output = `${cwd}/output/arquivos.txt`


export async function  concatenarArquivos() {

  const files = (await readdir(filesDir))

  const streams = files.map(
      item => createReadStream(join(filesDir, item))
  )

  const combinedStreams = new StreamConcat(streams)

  const finalStream = createWriteStream(output)

  const  handleStream = new  Transform({
      transform: async  (chunk, encoding, cb) => {
          const data = (chunk)

        /* csvtojson({
            noheader: true,
          //  output: 'json'
          }).fromString(`${(chunk)}`)
            .then( async row => {
              const ww3 = row.map(r => {
                const tt = r.field1.split(';')
                
                gravarParquet(
                  tt[0],
                  tt[1],
                  tt[2],
                  tt[3],
                  tt[4],
                  tt[5],
                  tt[6],
                  tt[7],
                  tt[8],
                  tt[9],
                  tt[10],
                  tt[11],
                  tt[12],
                  tt[13],
                  tt[14],
                  tt[15],
                  tt[16],
                  tt[17],
                  tt[18],
                )
                            
              })
            })
        */
          return cb(null, JSON.stringify(`${chunk}`))
    }
})

await pipelineAsync(
    combinedStreams,
    handleStream,
    finalStream
)

log(`${files.length} files merged! on ${output}`)

  return true
}

async function gravarParquet(nameRow,calling_numberRow,called_numberRow,serial_numberRow,
  call_typeRow,
  call_durationRow,
  start_timeRow,
  imsiRow,
  switchRow,
  cell_inRow,
  cell_outRow,
  tecnologiaRow,
  file_nameRow,
  first_lacRow,
  last_lacRow,
  ggsn_addressRow,
  dateRow,
  in_stockRow
  
  ) {

  var schema = new parquet.ParquetSchema({
    name: { type: 'UTF8' },
    exchange_id: { type: 'UINT_64' },
    calling_number: { type: 'UINT_64' },
    called_number:  {  type: 'UINT_64' },
    serial_number:  {  type: 'UINT_64' },
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
    
  });
  
  const writeRow = await parquet.ParquetWriter.openFile(schema, './output/filesystem.parquet');

  await writeRow.appendRow( {
    name: nameRow,
    exchange_id: nameRow ,
    calling_number: calling_numberRow,
    called_number: called_numberRow,
    serial_number:  serial_numberRow,
    call_type: call_typeRow,
    call_duration: call_durationRow,
    start_time: start_timeRow ,
    imsi: imsiRow,
    switch: switchRow  ,
    cell_in: cell_inRow ,
    cell_out: cell_outRow ,
    tecnologia: tecnologiaRow ,
    file_name: file_nameRow,
    first_lac: first_lacRow,
    last_lac: last_lacRow ,
    ggsn_address: ggsn_addressRow ,
    date: dateRow ,
    in_stock: in_stockRow

  } )

  await writeRow.close();

  return true 
}

