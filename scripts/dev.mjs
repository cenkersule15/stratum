import {spawn} from 'node:child_process';
import {existsSync} from 'node:fs';
const args=process.argv.slice(2);const i=args.indexOf('--port');const port=i>=0?args[i+1]:'4173';
const python=existsSync('.venv/bin/python')?'.venv/bin/python':'python3';
const api=spawn(python,['-m','uvicorn','stratum.api:app','--host','127.0.0.1','--port','8091'],{stdio:'inherit',env:{...process.env,PYTHONPATH:'backend'}});
const web=spawn(process.execPath,['node_modules/vite/bin/vite.js','--host','127.0.0.1','--port',port,'--strictPort'],{cwd:'frontend',stdio:'inherit'});
// Vite's entry path is relative to its own working directory.
web.on('error',e=>{console.error(e);api.kill();process.exit(1)});
for(const sig of ['SIGINT','SIGTERM'])process.on(sig,()=>{api.kill(sig);web.kill(sig)});
api.on('exit',code=>{web.kill();process.exit(code||0)});

web.on('exit',code=>{api.kill();process.exit(code||0)});
