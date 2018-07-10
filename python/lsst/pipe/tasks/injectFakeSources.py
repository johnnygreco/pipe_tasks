#
# LSST Data Management System
# Copyright 2008-2016 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.afw.image as afwImage
from lsst.synpipe import PositionGalSimFakesTask
from .calibrate import CalibrateTask


__all__ = ['InjectFakeSourcesConfig', 'InjectFakeSourcesTask']


class InjectFakeSourcesConfig(pexConfig.Config):
    """Configuration for injectFakeSources"""
    catalogFileName = lsstConfig.Field(
        dtype=str,
        doc="File name of catalog containing fake sources",
    )
    maskPlaneName = lsst.pex.config.Field(
        dtype=str, default="FAKE",
        doc="""Mask plane to set on pixels affected by fakes.  
               Will be added if not already present.
            """,
    )
    calibrate = pexConfig.ConfigurableField(
        target=CalibrateTask,
        doc="""Task to perform astrometric and photometric calibration:
            - refine the WCS in the exposure
            - refine the Calib photometric calibration object in the exposure
            - detect sources, usually at low S/N
            """,
    )
    positionGalsimFakes = pexConfig.ConfigurableField(
        target=PositionGalSimFakesTask,
        doc="Temporarily use synpipe in do the injection"
    )

    def setDefaults(self):
        self.calibrate.doPhotoCal = False
        self.calibrate.doAstrometry = False
        self.calibrate.doDeblend = True
        self.calibrate.doApCorr = True


class InjectFakeSourcesTask(pipeBase.CmdLineTask):
    """!Inject fake sources into an exposure.

    @anchor InjectFakeSourcesTask_

    @section pipe_tasks_injectFakeSources_Contents  Contents

     - @ref pipe_tasks_injectFakeSources_Purpose
     - @ref pipe_tasks_injectFakeSources_Initialize
     - @ref pipe_tasks_injectFakeSources_IO
     - @ref pipe_tasks_injectFakeSources_Config
     - @ref pipe_tasks_injectFakeSources_Debug
    """

    ConfigClass = InjectFakeSourcesConfig
    _DefaultName = "injectFakeSources"

    def __init__(self, butler=None, astromRefObjLoader=None,
                 psfRefObjLoader=None, photoRefObjLoader=None, **kwargs)
        """!Construct an InjectFakeSourcesTask

        @param[in] butler  The butler is passed to the refObjLoader constructor
            in case it is needed.  Ignored if the refObjLoader argument
            provides a loader directly.
        @param[in] astromRefObjLoader  An instance of LoadReferenceObjectsTasks
            that supplies an external reference catalog for astrometric
            calibration.  May be None if the desired loader can be constructed
            from the butler argument or all steps requiring a reference catalog
            are disabled.
        @param[in] photoRefObjLoader  An instance of LoadReferenceObjectsTasks
            that supplies an external reference catalog for photometric
            calibration.  May be None if the desired loader can be constructed
            from the butler argument or all steps requiring a reference catalog
            are disabled.
        @param[in,out] kwargs  other keyword arguments for
            lsst.pipe.base.CmdLineTask
        """


        pipeBase.CmdLineTask.__init__(self, **kwargs)

        # add fake source mask plane 
        afwImage.Mask[afwImage.MaskPixel]\
            .addMaskPlane(self.config.maskPlaneName)
        self.bitmask = afwImage.Mask[afwImage.MaskPixel]\
            .getPlaneBitMask(self.config.maskPlaneName)

        self.makeSubtask("calibrate", 
                         butler=butler, 
                         icSourceSchema=SourceTable.makeMinimalSchema(), 
                         astromRefObjLoader=astromRefObjLoader, 
                         photoRefObjLoader=photoRefObjLoader)
        
        self.makeSubtask("positionGalSimFakes", galList=self.catalogFileName)
    
    @pipeBase.timeMethod
    def run(self, dataRef):
        """!Inject fake sources in an exposure.

        @param[in] dataRef  butler data reference corresponding to a science
            image

        @return pipe_base Struct containing these fields:
        - exposure: exposure with fake sources (an lsst.afw.image.ExposureF)
        - sourceCat:  catalog of measured sources
        """
        self.log.info("Injecting fakes into {}".format(dataRef.dataId))

        exposure = dataRef.get("icExp", immediate=True)
        background = dataRef.get("icExpBackground", immediate=True)
        icSourceCat = dataRef.get("icSrc", immediate=True)

        # inject fakes using synpipe for now
        self.positionGalSimFakes.run(exposure, background=background)

        # detect, deblend, and apcorr using calibrate task
        calibRes = self.calibrate.run(
            dataRef=dataRef,
            exposure=exposure,
            background=background,
            doUnpersist=False,
            icSourceCat=icSourceCat
        )

        return pipeBase.Struct(
            exposure=calibRes.exposure,
            sourceCat=calibRes.sourceCat
        )
