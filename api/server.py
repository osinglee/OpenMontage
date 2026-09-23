import asyncio
import os
import uuid
import httpx
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(
    title="OpenMontage Agent API Server",
    version="1.0.0",
    description="AI 驱动的自动化音视频创作与流水线调度服务"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 内存中记录进行中的任务状态
PIPELINE_TASKS: dict[str, dict] = {}


class PipelineStartRequest(BaseModel):
    project_id: str
    skill_type: str = Field(default="ad-marketing")
    brief: dict = Field(default_factory=dict)
    callback_url: str | None = None


async def run_pipeline_worker(project_id: str, skill_type: str, brief: dict, callback_url: str | None):
    """异步流水线执行器，驱动 stages 演进并实时上报"""
    stages = [
        ("script", 20, "AI 编剧正在解析意图并生成分镜脚本与台词..."),
        ("scene_plan", 40, "分镜导演正在规划镜头景别、光影与色彩调度..."),
        ("assets", 65, "正在调用 Doubao-Seed3D-2.0 渲染高精度视觉原画..."),
        ("edit", 85, "正在调用 Doubao-Seedance-2.5 生成高帧率动态视频..."),
        ("compose", 95, "正在进行视听对齐、转场特效渲染与最终压制..."),
        ("publish", 100, "成片已就绪！")
    ]

    for stage_name, percent, msg in stages:
        await asyncio.sleep(2)  # 模拟各阶段生成与真实模型等待
        PIPELINE_TASKS[project_id] = {
            "stage": stage_name,
            "percent": percent,
            "message": msg,
            "status": "running"
        }
        if callback_url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(
                        callback_url,
                        json={
                            "project_id": project_id,
                            "stage": stage_name,
                            "percent": percent,
                            "message": msg,
                            "status": "running"
                        }
                    )
            except Exception as e:
                print(f"Callback notify error: {e}")

    # 完成交付产物
    if skill_type == "ad-marketing":
        image_url = "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=1080&q=80"
        video_url = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4"
    else:
        image_url = "https://images.unsplash.com/photo-1578632767115-351597cf2477?w=1080&q=80"
        video_url = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4"

    final_artifacts = [
        {
            "type": "image",
            "oss_key": f"projects/{project_id}/keyframe_1.jpg",
            "url": image_url,
            "thumbnail_url": image_url,
            "metadata": {"model": "Doubao-Seed3D-2.0", "role": "keyframe"}
        },
        {
            "type": "video",
            "oss_key": f"projects/{project_id}/final_render.mp4",
            "url": video_url,
            "thumbnail_url": image_url,
            "duration": 15.0,
            "metadata": {"model": "Doubao-Seedance-2.5", "resolution": "1080p"}
        }
    ]

    PIPELINE_TASKS[project_id] = {
        "stage": "publish",
        "percent": 100,
        "message": "生成完毕！",
        "status": "completed",
        "artifacts": final_artifacts
    }

    if callback_url:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    callback_url,
                    json={
                        "project_id": project_id,
                        "stage": "publish",
                        "percent": 100,
                        "message": "生成完毕！",
                        "status": "completed",
                        "artifacts": final_artifacts
                    }
                )
        except Exception as e:
            print(f"Final callback notify error: {e}")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "openmontage-agent", "version": "1.0.0"}


@app.post("/api/pipeline/start")
async def start_pipeline(req: PipelineStartRequest, background_tasks: BackgroundTasks):
    PIPELINE_TASKS[req.project_id] = {
        "stage": "idea",
        "percent": 10,
        "message": "初始化流水线...",
        "status": "running"
    }
    background_tasks.add_task(
        run_pipeline_worker,
        req.project_id,
        req.skill_type,
        req.brief,
        req.callback_url
    )
    return {"status": "accepted", "project_id": req.project_id}


@app.get("/api/pipeline/{project_id}/status")
async def get_pipeline_status(project_id: str):
    task = PIPELINE_TASKS.get(project_id)
    if not task:
        return {"status": "idle", "percent": 0, "message": "未找到流水线任务"}
    return task


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.server:app", host="0.0.0.0", port=8001, reload=True)
