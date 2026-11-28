from flask import make_response


def sweep_audio_response(wav_bytes, target_rpm, metadata, extra_headers=None):
    response = make_response(wav_bytes)
    response.headers['Content-Type'] = 'audio/wav'
    response.headers['Content-Disposition'] = f'attachment; filename="sweep_audio_rpm{int(target_rpm)}.wav"'
    response.headers['X-Target-RPM'] = str(metadata.get('target_rpm'))
    response.headers['X-Duration-Sec'] = str(metadata.get('duration_sec'))
    response.headers['X-Sample-Rate'] = str(metadata.get('sample_rate'))
    for name, value in (extra_headers or {}).items():
        response.headers[name] = str(value)
    return response
