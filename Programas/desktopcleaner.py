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

TIMESYMBOLS = {'customary' : ('s', 'm', 'h', 'D', 'w', 'M', 'Y'),'customary_ext' : ('sec', 'min', 'hour', 'day', 'week', 'month', 'year'),}

def human2seconds(s):
	init = s
	prefix= {}
	prefix['s']=1
	prefix['m']=60
	prefix['h']=3600
	prefix['D']=86400
	prefix['w']=604800
	prefix['M']=2592000
	prefix['Y']=31104000
	num = ""
	while s and s[0:1].isdigit() or s[0:1] == '.':
		num += s[0]
		s = s[1:]
	num = float(num)
	letter = s.strip()
	for name, sset in TIMESYMBOLS.items():
		if letter in sset:
			break
	else:
		raise ValueError("can't interpret %r" % init)
	return int(num * prefix[letter])

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

def removeLost(origen):
	global db
	perdidos = []
	cursor = db.cursor()
	cursor.execute('SELECT path FROM files WHERE lastcheck < '+ str(time.time() - human2seconds(opt.r)))
	for row in cursor.fetchall():
		if row[0].startswith(origen):
			perdidos.append(row[0])
			print 'archivo perido ' + row[0]
	for perdido in perdidos:
		print 'olvidando '+ perdido
		cursor.execute('DELETE FROM files WHERE path="'+ perdido +'"')
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
					if time.time() - fechaModificado > human2seconds(opt.r):
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
	if opt.deleteempty:
		removeIfEmpty(dir, 1)

parser = argparse.ArgumentParser(prog='desktopcleaner')
parser.add_argument('-o', action="append", required=True)
parser.add_argument('-d', action="append", required=True)
parser.add_argument('-deleteempty', default=True)
parser.add_argument('-r', default='1w')
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
		removeLost(origen)
db.close()
