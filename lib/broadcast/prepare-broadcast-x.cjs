// Worker-local export only. No provider, database, publication or generation calls.
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto'), cp = require('node:child_process');
if (process.env.VERCEL) throw Error('FFmpeg preparation is local only');
const [input, output] = process.argv.slice(2);
if (!input || !output) throw Error('Usage: node scripts/prepare-broadcast-x.cjs SOURCE.mp4 NEW-OUTPUT.mp4');
const source = path.resolve(input), destination = path.resolve(output);
if (source === destination || fs.existsSync(destination) || path.extname(destination).toLowerCase() !== '.mp4') throw Error('A new MP4 output path is required');
const ffmpeg = process.env.AIVE_SHORTS_FFMPEG || String.raw`C:\Users\royan\Wan2GP\ffmpeg_bins\ffmpeg.exe`;
const ffprobe = path.join(path.dirname(ffmpeg), process.platform === 'win32' ? 'ffprobe.exe' : 'ffprobe');
const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const probe = file => JSON.parse(cp.execFileSync(ffprobe, ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', file], { encoding: 'utf8', timeout: 30000 }));
const before = hash(source), original = probe(source), video = original.streams.find(s => s.codec_type === 'video');
if (!video || video.width * 16 !== video.height * 9 || !original.streams.some(s => s.codec_type === 'audio')) throw Error('V1 requires a 9:16 source with audio; cropping is prohibited');
const [rateNumerator,rateDenominator] = String(video.avg_frame_rate).split('/').map(Number);
const frameRate=rateNumerator/rateDenominator;
if (!Number.isFinite(frameRate) || frameRate<=0 || frameRate>40 || Number(original.format.duration)<=0 || Number(original.format.duration)>140
  || video.side_data_list?.some(item => item.rotation && item.rotation!==0)) throw Error('Source timing or rotation requires review; no automatic content changes');
cp.execFileSync(ffmpeg, ['-v', 'error', '-n', '-i', source, '-map', '0:v:0', '-map', '0:a:0',
  '-vf', 'scale=720:1280:flags=lanczos,setsar=1', '-c:v', 'libx264', '-profile:v', 'high', '-pix_fmt', 'yuv420p',
  '-preset', 'medium', '-b:v', '5500k', '-minrate', '5500k', '-maxrate', '5500k', '-bufsize', '11000k',
  '-x264-params', 'nal-hrd=cbr:force-cfr=1:open-gop=0', '-g', '60', '-c:a', 'aac', '-b:a', '192k', '-ar', '48000',
  '-movflags', '+faststart', destination], { timeout: 300000, windowsHide: true });
const result = probe(destination), v = result.streams.find(s => s.codec_type === 'video'), a = result.streams.find(s => s.codec_type === 'audio');
if (hash(source) !== before || v?.width !== 720 || v?.height !== 1280 || v?.codec_name !== 'h264' || a?.codec_name !== 'aac'
  || Math.abs(Number(result.format.duration) - Number(original.format.duration)) > 0.05
  || (video.nb_frames && v.nb_frames !== video.nb_frames)) throw Error('Export validation failed; do not publish');
cp.execFileSync(ffmpeg, ['-v', 'error', '-i', destination, '-f', 'null', '-'], { timeout: 300000, stdio: ['ignore', 'ignore', 'pipe'] });
const evidence = { sourceSha256: before, outputSha256: hash(destination), duration: Number(result.format.duration), width: v.width, height: v.height,
  frames: v.nb_frames, videoCodec: v.codec_name, audioCodec: a.codec_name, profile: 'x-portrait-720-v1', publicationAuthorized: false };
fs.writeFileSync(destination + '.verification.json', JSON.stringify(evidence, null, 2), { flag: 'wx' });
console.log(JSON.stringify(evidence));
