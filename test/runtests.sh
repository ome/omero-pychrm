#!/bin/sh
export ICE_CONFIG=ice.config
exec python -munittest test_TableConnection test_FeatureHandler

