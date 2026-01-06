if [ -z "$PYRSS_PYTHON_PATH" ]
then
    export PYRSS_PYTHON_PATH=$PYRSS_DIR/src
    export PYTHONPATH=$PYRSS_PYTHON_PATH:$PYTHONPATH
fi


alias pyrss="python3 -m pyrss"
