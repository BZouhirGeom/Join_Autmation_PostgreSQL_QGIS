"""
***************************************************************************
!/bin/python
-*- coding: utf-8 -*
QGIS Version: QGIS 3.16


 BELKHERROUBI Zouhir
 zouhirbelkherroubi@gmail.com
 B&Z PRO

# Description ###
Analyse et exportation des donn√©es apr√®s une des jointures sp√©cifiques entre diff√©rentes table de la BDD
***************************************************************************
"""
#### import the library
from qgis.PyQt.QtCore import QCoreApplication
from qgis.PyQt.QtGui import *

from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingAlgorithm,
                       QgsProcessingException,
                       QgsProcessingMultiStepFeedback,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterRasterLayer,
                       QgsProcessingParameterNumber,
                       QgsProcessingParameterFileDestination,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterField,
                       QgsProcessingParameterString)

from qgis import processing
from qgis.utils import iface
from PyQt5.QtSql import *
from qgis.core import (QgsVectorLayer,
                       QgsSymbol,
                       QgsRendererCategory, 
                       QgsCategorizedSymbolRenderer,
                       QgsProject,
                       QgsDataSourceUri,
                       QgsCoordinateReferenceSystem,
                       QgsProcessingParameterFileDestination)


###### The tool for the Qsp calcul: 
class Auotmation_BDD_QGIS(QgsProcessingAlgorithm):

    def translatString(self, string):
        """
        Returns a translatable string with the self.translatString() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return Auotmation_BDD_QGIS()

    def name(self):
       
        return 'Auotmation_BDD_QGIS'

    def displayName(self):
        
        return self.translatString('Auotmation_BDD_QGIS')

    def group(self):
        
        return self.translatString('Auotmation_BDD_QGIS')

    def groupId(self):
        
        return ''

    def shortHelpString(self):
        """
        Returns a localised short helper string for the algorithm. This string
        should provide a basic description about what the algorithm does and the
        parameters and outputs associated with it..
        """
        return self.translatString("""Cet algorithme permet de connecter QGIS ‡ une base de donnÈes PostgreSQL, rÈaliser une jointure attributaire entre deux couches (Tables) et exporter le rÈsultat en tableau sous format CSV  """)
    
    ######## define the inputs and output of the algorithm
    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterString('database', 'Database', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('port', 'Port', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('host', 'Host', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('user', 'User', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('password', 'Password', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('schema', 'Schema', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('dataprovider', 'Dataprovider', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('tablename_Text', 'Table de donn√©es Text', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('tablename_Geom', 'Table de donn√©es Geom', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('Field_J_Polygone', 'Join Field (Geom)', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('Field_J_Table', 'Join Field (Text)', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterString('File', 'Fields to copy', multiLine=False, defaultValue=''))
        self.addParameter(QgsProcessingParameterFileDestination('Tab', 'BDD Join Tab', optional=False, fileFilter='Fichiers CSV (*.csv)' , createByDefault=True, defaultValue=None))


    ########Processing algorithm
    def processAlgorithm(self, parameters, context, feedback):
        feedback = QgsProcessingMultiStepFeedback(2, feedback)
        results = {}
        outputs = {}
        
        ##Algorithme
        ##define the table 1
        uri = QgsDataSourceUri()
        # BDD connexion parameters
        uri.setConnection(parameters['host'], parameters['port'], parameters['database'], parameters['user'], parameters['password'])
        # Table 1 parameters
        uri.setDataSource (parameters['schema'], parameters['tablename_Geom'], "geom")
        Geom_layer=QgsVectorLayer(uri.uri(), parameters['tablename_Geom'], parameters['dataprovider'])
        ##define the table 2
        uri2 = QgsDataSourceUri()
        # BDD connexion parameters
        uri2.setConnection(parameters['host'], parameters['port'], parameters['database'], parameters['user'], parameters['password'])
        # Table 2 parameters
        uri2.setDataSource (parameters['schema'], parameters['tablename_Text'], "geom")
        Text_layer=QgsVectorLayer(uri.uri(), parameters['tablename_Text'], parameters['dataprovider'])
        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}
        
        # Reprojeter une couche
        Reproject = processing.run('native:reprojectlayer', {
        'INPUT': parameters['tablename_Geom'],
        'OPERATION': '',
        'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:2154'),
        'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT},
        context=context, feedback=feedback)
        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}
        
        ###JOIN
        # Join attributes by field value
        Join_BDD = processing.run('native:joinattributestable', {
        'DISCARD_NONMATCHING': False,
        'FIELD': parameters['Field_J_Polygone'],
        'FIELDS_TO_COPY': parameters['File'],
        'FIELD_2': parameters['Field_J_Table'],
        'INPUT': Reproject['OUTPUT'],
        'INPUT_2': parameters['tablename_Text'],
        'METHOD': 1,
        'OUTPUT': parameters['Tab'],
        'PREFIX': 'J_'},
        context=context, feedback=feedback)
        
        results['Join'] = Join_BDD['OUTPUT']
        
        return results
        
       
    
    
