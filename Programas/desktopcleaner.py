import sqlite3
import os
import hashlib
import time
import shutil
import argparse
import fnmatch
import re
import itertools
import logging

def hashArchivo(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def removeIfEmpty(dir, raiz=0):
	for entry in os.listdir(dir):
		if os.path.isdir(os.path.join(dir, entry)):
			removeIfEmpty(os.path.join(dir, entry))
	if raiz == 0:
		if not os.listdir(dir):
			print 'borrando directorio vacio '+ dir
			try:
				os.rmdir(dir)		
			except:
				print 'error al borrar directorio '+ dir

def crearDirectorios(ruta):
	if os.path.isdir(ruta):
		print 'el directorio ya existe '+ ruta 
	else:
		print 'crear directorio '+ ruta 
		os.makedirs(ruta)

			
def removeLost():
	global db
	cursor = db.cursor()
	cursor.execute('SELECT path FROM files WHERE lastcheck < '+ str(time.time() - 604800))
	for row in cursor.fetchall():
		print 'archivo perido ' + row[0]
	cursor.execute('DELETE FROM files WHERE lastcheck < '+ str(time.time()  - 604800))
	db.commit()
	
	
def buscarArchivos(dir, destino):
	global db
	cursor = db.cursor()
	if opt.exclude:
		excludes = list(itertools.chain(*opt.exclude))	
		rexcludes = r'|'.join([fnmatch.translate(x) for x in excludes]) or r'$.'
		
	for root, dirs, files in os.walk(dir):
		print 'buscando en '+ root
		if opt.exclude:
			dirs[:] = [d for d in dirs if not re.match(rexcludes, d)]
			files[:] = [f for f in files if not re.match(rexcludes, f)]
		
		for file in files:
			archivoEncontrado = 0
			archivoModificado = 0
			fechaModificado = 0
			idActual = 0
			rutaActual = str(os.path.join(root, file))
			try:
				hashActual = hashArchivo(rutaActual)
			except:
				continue
				
			tamanioActual = os.path.getsize(rutaActual)
			cursor.execute('SELECT id, path, hash, lastdiff FROM files WHERE path="'+rutaActual+'"')
			for row in cursor.fetchall():
				archivoEncontrado = 1
				idActual = row[0]
				fechaModificado = row[3]
				if not row[2] == hashActual:
					archivoModificado = 1
			if archivoEncontrado:
				if archivoModificado:
					print 'archivo modificado '+ rutaActual
					cursor.execute('UPDATE files SET size=?, hash=?, lastcheck=?, lastdiff=? WHERE id=?', (tamanioActual, hashActual, time.time(), time.time(), idActual))
					db.commit()
				else:
					if time.time() - fechaModificado > 604800:
						print 'mover archivo '+ rutaActual
						dirDestino = os.path.normpath(str(root).replace(dir, destino))
						crearDirectorios(dirDestino)
						try:
							shutil.move(rutaActual, os.path.join(dirDestino, file))
						except:
							print 'error al mover archivo '+ rutaActual
							logger.error('error al mover el archivo '+ rutaActual)
						else:
							if os.path.isfile(os.path.join(dirDestino, file)):
								logger.info('archivo movido '+ rutaActual +' a '+ dirDestino)
								cursor.execute('DELETE FROM files WHERE id='+ str(idActual))
								db.commit()		
					else:
						cursor.execute('UPDATE files SET lastcheck=? WHERE id=?', (time.time(), idActual))
						db.commit()
			else:
				print 'nuevo archivo '+ rutaActual
				cursor.execute('INSERT INTO files(path, name, size, hash, lastcheck, lastdiff) VALUES(?,?,?,?,?,?)', (rutaActual, file, tamanioActual, hashActual, time.time(), time.time() ))
				db.commit()
	removeIfEmpty(dir, 1)

parser = argparse.ArgumentParser(prog='pybackup')
parser.add_argument('-o', action="append", required=True)
parser.add_argument('-d', action="append", required=True)
parser.add_argument('-maxsize', default='100M')
parser.add_argument('-n', type=int, default=3)
parser.add_argument('-r', default='1M')
parser.add_argument('-exclude', action="append", nargs='*')

opt = parser.parse_args()				

	
	
db = sqlite3.connect('desktopcleaner.db')
db.text_factory = lambda x: unicode(x, "utf-8", "ignore")

cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, path TEXT, name TEXT, size INTEGER, hash TEXT, lastcheck INTEGER, lastdiff INTEGER)')
db.commit()


for logdir in opt.d:
	if not logdir.endswith(os.path.sep):
		logdir = logdir + os.path.sep 
	if not (os.path.isdir(logdir)):
		os.makedirs(logdir)

logger = logging.getLogger('desktopcleaner')
hdlr = logging.FileHandler(logdir + 'desktopcleaner.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


for destino in opt.d:
	for origen in opt.o:
		if not origen.endswith(os.path.sep):
			origen = origen + os.path.sep
		if not destino.endswith(os.path.sep):
			destino = destino + os.path.sep		
		buscarArchivos(origen, destino)
		

removeLost()
db.close()
