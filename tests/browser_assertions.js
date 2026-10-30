/* Assertions and isolated scopes for Vanilla JS regression fixtures in Chromium.
 * Python supplies read-only source text explicitly; no filesystem/network APIs
 * or server-side JavaScript runtime are exposed to a fixture.
 */
(() => {
  const nativeConsole=console,realSetTimeout=setTimeout.bind(window),nativeCrypto=crypto;
  const browserValue=(key,realm=window)=>key==='structuredClone'?window.structuredClone.bind(window):['setTimeout','clearTimeout','setInterval','clearInterval','requestAnimationFrame',
    'cancelAnimationFrame','structuredClone','atob','btoa','fetch'].includes(key)?realm[key].bind(realm):
    ['window','document','location','self','top','parent','frames','navigator','sessionStorage','localStorage','history','performance'].includes(key)?realm[key]:window[key];
  const equal=(a,b,seen=new Map())=>{
    if(Object.is(a,b))return true;
    if(!a||!b||typeof a!=='object'||typeof b!=='object')return false;
    if(Object.prototype.toString.call(a)!==Object.prototype.toString.call(b))return false;
    if(Object.getPrototypeOf(a)!==Object.getPrototypeOf(b))return false;
    if(a instanceof Date)return +a===+b;
    if(a instanceof RegExp)return String(a)===String(b);
    if(seen.has(a))return seen.get(a)===b;
    const nested=new Map(seen);nested.set(a,b);
    const tag=Object.prototype.toString.call(a);
    if(tag==='[object Set]'||tag==='[object Map]'){
      if(a.size!==b.size)return false;
      const remaining=Array.from(b);
      for(const value of a){const index=remaining.findIndex(other=>equal(value,other,nested));
        if(index<0)return false;remaining.splice(index,1);}return true;
    }
    if(ArrayBuffer.isView(a)||tag==='[object ArrayBuffer]'){
      if(a.byteLength!==b.byteLength)return false;
      const av=new Uint8Array(a.buffer||a,a.byteOffset||0,a.byteLength),bv=new Uint8Array(b.buffer||b,b.byteOffset||0,b.byteLength);
      return av.every((value,index)=>value===bv[index]);
    }
    if(/\[object (?:Number|String|Boolean|BigInt)\]/.test(tag)&&!Object.is(a.valueOf(),b.valueOf()))return false;
    if(/Error\]$/.test(tag)&&(a.name!==b.name||a.message!==b.message||!equal(a.cause,b.cause,nested)))return false;
    const keys=value=>Reflect.ownKeys(value).filter(key=>Object.prototype.propertyIsEnumerable.call(value,key));
    const ak=keys(a),bk=keys(b);
    return ak.length===bk.length&&ak.every(k=>Object.hasOwn(b,k)&&equal(a[k],b[k],nested));
  };
  const display=value=>{try{return JSON.stringify(value);}catch{return String(value);}};
  const assert=(value,message)=>{if(!value)throw Error(message||'Expected truthy value');};
  assert.ok=assert;
  assert.fail=message=>{throw Error(message||'Explicit assertion failure');};
  assert.equal=assert.strictEqual=(actual,expected,message)=>assert(Object.is(actual,expected),message||`Expected ${display(expected)}, got ${display(actual)}`);
  assert.notEqual=assert.notStrictEqual=(actual,expected,message)=>assert(!Object.is(actual,expected),message||`Unexpected ${display(actual)}`);
  assert.deepEqual=assert.deepStrictEqual=(actual,expected,message)=>assert(equal(actual,expected),message||`Expected deep equality: ${display(actual)} != ${display(expected)}`);
  assert.match=(value,pattern,message)=>assert(pattern.test(value),message||`Expected ${display(value)} to match ${pattern}`);
  assert.doesNotMatch=(value,pattern,message)=>assert(!pattern.test(value),message||`Unexpected ${pattern} in ${display(value)}`);
  const checkError=(error,expected)=>{
    if(!expected)return true;
    if(expected instanceof RegExp)return expected.test(String(error.message||error));
    if(typeof expected==='function')return expected.prototype instanceof Error||expected===Error?error instanceof expected:expected(error);
    return Object.entries(expected).every(([key,value])=>value instanceof RegExp?value.test(error[key]):error[key]===value);
  };
  assert.throws=(fn,expected,message)=>{try{fn();}catch(error){assert(checkError(error,expected),message||String(error));return error;}throw Error(message||'Expected an exception');};
  assert.doesNotThrow=fn=>fn();
  assert.rejects=async(fn,expected,message)=>{try{await(typeof fn==='function'?fn():fn);}catch(error){assert(checkError(error,expected),message||String(error));return;}throw Error(message||'Expected a rejected promise');};

  // Top-level binding discovery only supports fixture exports, not application
  // interpretation: Chromium itself parses and executes every source fragment.
  function bindings(source){
    const tokens=[];let brace=0,paren=0,bracket=0,i=0,previous='';
    while(i<source.length){
      const c=source[i],next=source[i+1];
      if(/\s/.test(c)){i++;continue;}
      if(c==='/'&&next==='/'){i=source.indexOf('\n',i+2);if(i<0)break;continue;}
      if(c==='/'&&next==='*'){const end=source.indexOf('*/',i+2);i=end<0?source.length:end+2;continue;}
      if(c==='"'||c==="'"||c==='`'){
        const quote=c;i++;while(i<source.length){if(source[i]==='\\'){i+=2;continue;}if(source[i++]===quote)break;}previous='literal';continue;
      }
      if(c==='/'&&(!previous||/[=(:,[!&|?;{}]/.test(previous.slice(-1))||['return','throw','case','=>'].includes(previous))){
        i++;let cls=false;while(i<source.length){const ch=source[i++];if(ch==='\\'){i++;continue;}if(ch==='[')cls=true;if(ch===']')cls=false;if(ch==='/'&&!cls)break;}while(/[a-z]/i.test(source[i]||'')&&i<source.length)i++;previous='literal';continue;
      }
      const match=source.slice(i).match(/^[A-Za-z_$][\w$]*/);
      if(match){const word=match[0];tokens.push({word,brace,paren,bracket});i+=word.length;previous=word;continue;}
      tokens.push({word:c,brace,paren,bracket});
      if(c==='{')brace++;if(c==='}')brace--;if(c==='(')paren++;if(c===')')paren--;if(c==='[')bracket++;if(c===']')bracket--;
      previous=c;i++;
    }
    const names=new Set();
    for(let n=0;n<tokens.length;n++){
      const t=tokens[n];if(t.brace||t.paren||t.bracket)continue;
      if(['function','class'].includes(t.word)){
        let word=tokens[n+1]?.word;if(word==='*')word=tokens[n+2]?.word;
        if(word!=='extends'&&/^[A-Za-z_$][\w$]*$/.test(word||''))names.add(word);
      }
      if(['var','let','const'].includes(t.word)){
        const first=tokens[n+1];if(first&&/^[A-Za-z_$][\w$]*$/.test(first.word))names.add(first.word);
        for(let j=n+2;j<tokens.length;j++){
          const q=tokens[j];if(q.brace===0&&q.paren===0&&q.bracket===0){
            if(q.word===';')break;
            if(q.word===','&&/^[A-Za-z_$][\w$]*$/.test(tokens[j+1]?.word||''))names.add(tokens[j+1].word);
          }
        }
      }
    }return Array.from(names);
  }
  function scope(source,context,globals){
    const names=bindings(source);
    const proxy=new Proxy(context,{
      has:(target,key)=>!['__scope','__source','eval'].includes(key)&&!names.includes(key)&&
        (key in target||key in globals||key in globals.__realm),
      get:(target,key)=>key===Symbol.unscopables?undefined:key in target?target[key]:key in globals?globals[key]:browserValue(key,globals.__realm),
      set:(target,key,value)=>{target[key]=value;return true;}
    });
    const exports=names.map(name=>`try{Object.defineProperty(__scope,${JSON.stringify(name)},{configurable:true,enumerable:true,get:()=>${name},set:v=>{${name}=v;}});}catch{}`).join('\n');
    return new Function('__scope','__source',`with(__scope){return eval(__source);}`)(proxy,source+(exports?'\n;'+exports:''));
  }

  window.__pbguiRunFixture=async function({source,args,input,cwd,filename,files,checkOnly}){
    const frame=document.createElement('iframe');frame.hidden=true;document.body.appendChild(frame);
    const realm=frame.contentWindow;
    const output=[],errors=[],testResult={exitCode:0},immediates=new Set(),after=[];
    const normalize=path=>{
      const parts=[];for(const part of String(path).split('/')){if(!part||part==='.')continue;if(part==='..')parts.pop();else parts.push(part);}return '/'+parts.join('/');};
    const absolute=path=>normalize(String(path).startsWith('/')?path:cwd+'/'+path);
    const testSources={readFileSync(path){if(path===0)return input||'';const full=absolute(path);
      if(!Object.hasOwn(files,full))throw Error('Fixture source was not explicitly supplied: '+full);return files[full];},
      existsSync:path=>Object.hasOwn(files,absolute(path)),
      readdirSync:path=>Array.from(new Set(Object.keys(files).filter(key=>key.startsWith(absolute(path)+'/'))
        .map(key=>key.slice(absolute(path).length+1).split('/')[0]))).sort()};
    const sourcePath={join:(...parts)=>normalize(parts.join('/')),resolve:(...parts)=>absolute(parts.join('/')),
      dirname:path=>normalize(path).split('/').slice(0,-1).join('/')||'/',basename:path=>String(path).split('/').at(-1)};
    const console={log:(...values)=>output.push(values.map(v=>typeof v==='string'?v:display(v)).join(' ')),
      error:(...values)=>errors.push(values.map(v=>v?.stack||String(v)).join(' ')),warn:()=>{},info:()=>{},debug:()=>{}};
    const globals={__realm:realm,console,browserAssert:assert,testSources,sourcePath,browserCrypto:{webcrypto:nativeCrypto,randomUUID:()=>nativeCrypto.randomUUID()},
      testArguments:['browser-fixture',...(filename?[filename]:[]),...args],testResult,
      testInput:{setEncoding(){},on(name,fn){if(name==='data')queueMicrotask(()=>fn(input||''));if(name==='end')queueMicrotask(fn);}},
      testErrors:{write:value=>errors.push(String(value))},
      testOutput:{write:value=>output.push(String(value))},
      afterFixture:callback=>after.push(callback),
      finishTest(code=0){testResult.exitCode=code;if(code)throw Error('Fixture exited with '+code);},
      setImmediate:fn=>{const id=realSetTimeout(()=>{try{fn();}catch(error){testResult.exitCode=1;errors.push(error.stack||String(error));}finally{immediates.delete(id);}},0);immediates.add(id);return id;},
      clearImmediate:id=>{clearTimeout(id);immediates.delete(id);},
      __dirname:sourcePath.dirname(filename||cwd+'/inline.js'),__filename:filename||cwd+'/inline.js'};
    globals.global=globals;globals.globalThis=globals;
    globals.browserScope={createContext:context=>context,runInContext:(text,context)=>scope(text,context,globals),
      runInNewContext:(text,context)=>scope(text,context,globals),runInThisContext:text=>scope(text,globals,globals),
      Script:class {constructor(text){new Function(text);}}};
    globals.loadBrowserModule=path=>{const module={exports:{}};
      scope(testSources.readFileSync(path),{module,exports:module.exports,window:globals,globalThis:globals},globals);return module.exports;};
    let result;
    try{
      if(checkOnly)new Function(source);
      else{
        const names=bindings(source);
        const aliases=names.map(name=>`Object.defineProperty(global,${JSON.stringify(name)},{configurable:true,enumerable:true,get:()=>${name},set:v=>{${name}=v;}});`).join('\n');
        const proxy=new Proxy(globals,{has:(t,k)=>!['__scope','__source','eval'].includes(k)&&!names.includes(k)&&(k in t||k in realm),
          get:(t,k)=>k===Symbol.unscopables?undefined:k in t?t[k]:browserValue(k,realm),set:(t,k,v)=>{t[k]=v;return true;}});
        const strict=source.match(/^\s*(['"])use strict\1\s*;/);
        const program=(strict?strict[0]+'\n':'')+aliases+'\n'+(strict?source.slice(strict[0].length):source);
        result=new Function('__scope','__source','with(__scope){return eval(__source);}')(
          proxy,program);
        await result;
        while(immediates.size)await new Promise(resolve=>realSetTimeout(resolve,0));
        for(const callback of after)await callback();
      }
    }catch(error){testResult.exitCode=1;errors.push(error.stack||String(error));}
    frame.remove();
    return {returncode:Number(testResult.exitCode)||0,stdout:output.join('\n')+(output.length?'\n':''),stderr:errors.join('\n')};
  };
})();
