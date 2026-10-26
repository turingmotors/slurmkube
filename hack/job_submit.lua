-- Minimal JobSubmit plugin for SlurmKube (slurm.conf: JobSubmitPlugins=lua).
--
-- Jobs submitted to a SlurmKube partition are held at submission with the
-- comment SlurmKube claims them by (config claim.heldComment). SlurmKube
-- creates the job's pods, pins the job to the dynamic nodes they register
-- and releases it. A site plugin will usually do more (defaults for CPUs
-- and memory per GPU, refusing options the nodes cannot satisfy — see the
-- the Slurm options the translation refuses); this is the part SlurmKube requires.

local PARTITIONS = { ["k8s"] = true }
local HELD_COMMENT = "slurmkube: held for admission"

function slurm_job_submit(job_desc, part_list, submit_uid)
  local part = job_desc.partition
  if part == nil or not PARTITIONS[part] then
    return slurm.SUCCESS
  end
  if job_desc.het_job_offset ~= nil and job_desc.het_job_offset ~= slurm.NO_VAL then
    slurm.log_user("slurmkube: heterogeneous jobs are not supported on " .. part)
    return slurm.ERROR
  end
  if job_desc.features ~= nil and job_desc.features ~= "" then
    slurm.log_user("slurmkube: --constraint is not supported on " .. part .. " (SlurmKube pins the job to its own nodes by feature)")
    return slurm.ERROR
  end
  job_desc.priority = 0            -- held
  job_desc.comment = HELD_COMMENT
  return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
  -- Users may not release or re-shape a SlurmKube-managed job themselves;
  -- SlurmKube (its slurmrestd user) does. Replace 0 with that user's uid.
  if job_rec.partition ~= nil and PARTITIONS[job_rec.partition] and modify_uid ~= 0 then
    if job_desc.priority ~= nil and job_desc.priority ~= slurm.NO_VAL then
      slurm.log_user("slurmkube: jobs on " .. job_rec.partition .. " are released by SlurmKube, not by hand")
      return slurm.ERROR
    end
  end
  return slurm.SUCCESS
end

return slurm.SUCCESS
