#!/bin/sh
export ICE_CONFIG=ice.config
# Python 2.7
exec python -munittest test_TableConnection test_PychrmStorage test_Pychrm

# Python 2.6
#exec python -munittest2.__main__ test_TableConnection test_PychrmStorage test_Pychrm

