/*
 *  Copyright (C) 2004-2026 Savoir-faire Linux Inc.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef __LIBAV_DEPS_H__
#define __LIBAV_DEPS_H__

// NOTE versions of FFmpeg's librairies can be checked using something like this:
// #if LIBAVCODEC_VERSION_INT >= AV_VERSION_INT( major, minor, micro )

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavfilter/avfilter.h>
#include <libavformat/avformat.h>
#include <libavdevice/avdevice.h>
#include <libswscale/swscale.h>
#include <libavutil/avutil.h>
#include <libavutil/time.h>
#include <libavutil/pixdesc.h>
#include <libavutil/opt.h>
#include <libavutil/channel_layout.h>
#include <libavutil/imgutils.h>
#include <libavutil/intreadwrite.h>
#include <libavutil/log.h>
#include <libavutil/samplefmt.h>

#if LIBAVUTIL_VERSION_MAJOR < 56
AVFrameSideData* av_frame_new_side_data_from_buf(AVFrame* frame, enum AVFrameSideDataType type, AVBufferRef* buf);
#endif
}

// FFmpeg 5.1+ uses ch_layout struct, older versions use channels/channel_layout
#if LIBAVUTIL_VERSION_INT >= AV_VERSION_INT(57, 28, 100)
// New FFmpeg 5.1+ channel layout API
#define JAMI_LIBAV_HAS_NEW_CHANNEL_LAYOUT 1
#define JAMI_LIBAV_NB_CHANNELS(obj) ((obj)->ch_layout.nb_channels)
#define JAMI_LIBAV_SET_CHANNELS(obj, nb) av_channel_layout_default(&(obj)->ch_layout, (nb))
#define JAMI_LIBAV_SET_CHANNEL_LAYOUT_FROM_MASK(obj, mask) av_channel_layout_from_mask(&(obj)->ch_layout, (mask))
#define JAMI_LIBAV_CHANNEL_LAYOUT_MASK(obj) ((obj)->ch_layout.u.mask)
#define JAMI_LIBAV_COPY_CHANNEL_LAYOUT(dst, src) av_channel_layout_copy(&(dst)->ch_layout, &(src)->ch_layout)
#else
// Legacy FFmpeg channel layout API
#define JAMI_LIBAV_HAS_NEW_CHANNEL_LAYOUT 0
#define JAMI_LIBAV_NB_CHANNELS(obj) ((obj)->channels)
#define JAMI_LIBAV_SET_CHANNELS(obj, nb) do { (obj)->channels = (nb); (obj)->channel_layout = av_get_default_channel_layout(nb); } while(0)
#define JAMI_LIBAV_SET_CHANNEL_LAYOUT_FROM_MASK(obj, mask) do { (obj)->channel_layout = (mask); (obj)->channels = av_get_channel_layout_nb_channels(mask); } while(0)
#define JAMI_LIBAV_CHANNEL_LAYOUT_MASK(obj) ((obj)->channel_layout)
#define JAMI_LIBAV_COPY_CHANNEL_LAYOUT(dst, src) do { (dst)->channel_layout = (src)->channel_layout; (dst)->channels = (src)->channels; } while(0)
#endif

#include "libav_utils.h"

#endif // __LIBAV_DEPS_H__
