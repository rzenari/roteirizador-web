# hook-ortools.py
from PyInstaller.utils.hooks import collect_dynamic_libs

# Encontra as bibliotecas dinâmicas (.dll), como antes
binaries = collect_dynamic_libs("ortools")

# Lista os 'módulos escondidos' que o PyInstaller não consegue ver sozinho
hiddenimports = [
    'ortools.constraint_solver.pywrapcp',
    'ortools.constraint_solver.routing_parameters_pb2'
]